package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.StreamResourcePair;
import eu.uk.ncl.di.pet5o.PATH2iot.input.energy.EnergyImpactCoefficients;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureResource;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.NodeCapability;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompPlacement;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlacementGenerator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlacementNode;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator handler class manages logical plan generation, enumeration of physical plans, and
 * returns the final execution plan.
 *
 * @author Peter Michalak
 */
public class OperatorHandler {

    private static Logger logger = LogManager.getLogger(OperatorHandler.class);

    private NeoHandler neoHandler;
    private LogicalPlan logicalPlan;
    private ArrayList<LogicalPlan> enumeratedLogicalPlans;
    private ArrayList<PhysicalPlan> enumeratedPhysicalPlans;

    public OperatorHandler(NeoHandler neoHandler) {
        this.neoHandler = neoHandler;
        enumeratedLogicalPlans = new ArrayList<>();
        enumeratedPhysicalPlans = new ArrayList<>();
    }

    /**
     * Search from the given node Id along STREAMS connections to get vertices which were not yet placed.
     * @param compId starting point
     * @return list of vertex Ids which were not yet placed.
     */
    private ArrayList<Integer> findCompsToPlace(Integer compId) {
        return neoHandler.findShortestPath(compId, "COMP", "COMP", "STREAMS", "PLACED");
    }

    /**
     * pulls all the nodes which were not yet placed and
     * if there are any, places them
     */
    private boolean isPlacementComplete() {
        return neoHandler.getNotPlacedNodes().size() == 0;
    }

    /**
     * checks whether the node has been placed already
     */
    private boolean isPlaced(Integer nodeId, String type, String type2) {
        ArrayList<Integer> hosts = neoHandler.getAllHosts(nodeId, type, type2, "PLACED");
        return hosts.size() != 0;
    }

    /**
     * @return infrastructure node if found
     */
    private InfrastructureNode getNodeById(InfrastructureDesc infra, int resourceId) {
        for (InfrastructureNode node : infra.getNodes()) {
            if (node.getResourceId() == resourceId) {
                return node;
            }
        }
        return null;
    }

    /**
     * Saves the placement information to a file
     * @param path path to file
     */
    public void savePhysicalPlan(String path) throws IOException {
        logger.debug("Collecting information to save the physical plan...");
        // get all computations
        ArrayList<Integer> compIds = neoHandler.getAllVertices("COMP");
        logger.debug(String.format("Gathered %d: %s", compIds.size(), compIds));

        // get all placement info
        ArrayList<CompPlacement> compPlacements = new ArrayList<>();
        for (Integer compId : compIds) {
            compPlacements.add(new CompPlacement(compId,
                    neoHandler.getCompPlacement(compId, "COMP", "PLACED", "NODE")));
        }

        // save to a file
        Gson gson = new Gson();
        String ppJson = gson.toJson(compPlacements);
        File file = new File(path);
        CharSink sink = Files.asCharSink(file, Charsets.UTF_8);
        sink.write(ppJson);
        logger.debug("Physical plan representation persisted: " + ppJson);
    }

    /**
     * Rebuild a graph of operators with direct links in between the computations.
     */
    public void createDirectLinks(String rel) {
        // find the beginning of the graph
        ArrayList<Integer> opInitialNodes = neoHandler.findNodesWithoutIncomingRelationship("COMP", "STREAMS");
        logger.debug("Operator initial nodes: " + opInitialNodes);

        // find the downstream operator
        for (Integer opInitialNode : opInitialNodes) {
            findDownstreamNodesRec(rel, opInitialNode);
        }
    }

    private void findDownstreamNodesRec(String rel, Integer opInitialNode) {
        ArrayList<Integer> downstreamNodes = neoHandler.findDownstreamNodes(opInitialNode,
                "COMP", "COMP", "STREAMS", 5);
        logger.debug(String.format("Node %d has following downstream nodes: %s", opInitialNode, downstreamNodes));

        // create a direct link
        for (Integer downstreamNode : downstreamNodes) {
            neoHandler.createRel(opInitialNode, rel, downstreamNode);
            logger.debug("Find direct downstream nodes for " + downstreamNode);
            findDownstreamNodesRec(rel, downstreamNode);
        }
    }

    /**
     * Loop through the physical placement option and return the last Infrastructure node.
     */
    private InfrastructureNode getLastInfrastructureNode(List<Object> physicalPlacement) {
        InfrastructureNode lastNode = null;
        for (Object element : physicalPlacement) {
            if (element.getClass().equals(InfrastructureNode.class)) {
                lastNode = (InfrastructureNode) element;
            }
        }
        return lastNode;
    }

    /**
     * Builds a logical plan representation from the current state within the neo4j
     * @param udfs
     */
    public void buildLogicalPlan(UdfDefs udfs) {
        logicalPlan = new LogicalPlan(neoHandler, udfs);

        // add the first plan to the collection
        enumeratedLogicalPlans.add(logicalPlan);
    }

    /**
     * @return number of logical plans
     */
    public int getLogicalPlanCount() {
        return enumeratedLogicalPlans.size();
    }

    /**
     * @return number of operators
     */
    public int getOpCount() {
        return logicalPlan.getOpCount();
    }

    /**
     * Applies given optimisation technique on initial logical plan,
     * generating additional plan options.
     */
    public ArrayList<LogicalPlan> applyLogicalOptimisation(LogicalPlan logicalPlan, String optiTechnique) {
        switch (optiTechnique) {
            case "win":
                return windowOptimisation(logicalPlan);
            case "select":
                return selectOptimisation(logicalPlan);
            default:
                logger.error(String.format("Given logical optimisation: %s is not supported.", optiTechnique));
        }
        return null;
    }

    /**
     * Applies pushing projects optimisation technique
     * * scans for project type operators in the logical plan
     * * creates a new logical plan when project operator is moved closer to the data source
     * @param logicalPlan logical plan to optimise
     * @return array list of new logical plans created
     */
    private ArrayList<LogicalPlan> selectOptimisation(LogicalPlan logicalPlan) {
        ArrayList<LogicalPlan> optimisedPlans = new ArrayList<>();
        if (logicalPlan.hasSelect()) {
            pushSelectCloserToTheDataSource(logicalPlan, optimisedPlans);
        }
        return optimisedPlans;
    }

    private void pushSelectCloserToTheDataSource(LogicalPlan logicalPlan, ArrayList<LogicalPlan> optimisedPlans) {
        // generate more plans
        CompOperator selectOp = logicalPlan.getSelect(0, "SELECT");
        LogicalPlan newLogPlan = logicalPlan.pushOperatorUpstream(selectOp);
        if (newLogPlan != null) {
            logger.info("New logical plan: " + newLogPlan);
            optimisedPlans.add(newLogPlan);

            // try to optimise the new plan
            selectOptimisationLoop(newLogPlan, optimisedPlans);
        }
    }

    private void selectOptimisationLoop(LogicalPlan logicalPlan, ArrayList<LogicalPlan> optimisedPlans) {
        // generate more plans
        CompOperator selectOp = logicalPlan.getOperator(0, "SELECT");
        LogicalPlan newLogPlan = logicalPlan.pushOperatorUpstream(selectOp);
        if (newLogPlan != null) {
            logger.info("New logical plan: " + newLogPlan);
            optimisedPlans.add(newLogPlan);

            // try to optimise the new plan
            windowOptimisationLoop(newLogPlan, optimisedPlans);
        }
    }

    /**
     * Applies window optimisation technique:
     * * scans for a window in the logical plan
     * * creates a new logical plans by pushing the window closer to the data source
     * @param logicalPlan logical plan to optimise
     */
    private ArrayList<LogicalPlan> windowOptimisation(LogicalPlan logicalPlan) {
        ArrayList<LogicalPlan> optimisedPlans = new ArrayList<>();
        if (logicalPlan.hasWindows()) {
            windowOptimisationLoop(logicalPlan, optimisedPlans);
        }
        return optimisedPlans;
    }

    /**
     * Recursively examine the logical plan until the source node is reached,
     * generating new logical plans.
     * @param logicalPlan plan to be examined
     * @param optimisedPlans list of all generated plans
     */
    private void windowOptimisationLoop(LogicalPlan logicalPlan, ArrayList<LogicalPlan> optimisedPlans) {
        // generate more plans
        CompOperator winOp = logicalPlan.getOperator(0, "win");
        LogicalPlan newLogPlan = logicalPlan.pushOperatorUpstream(winOp);
        if (newLogPlan != null) {
            logger.info("New logical plan: " + newLogPlan);
            optimisedPlans.add(newLogPlan);

            // try to optimise the new plan
            windowOptimisationLoop(newLogPlan, optimisedPlans);
        }
    }

    /**
     * @return initial logical plan
     */
    public LogicalPlan getInitialLogicalPlan() {
        return logicalPlan;
    }

    /**
     * Adds supplied plans to the internal list.
     * @param logicalPlans plans to be added
     */
    public void appendLogicalPlans(ArrayList<LogicalPlan> logicalPlans) {
        enumeratedLogicalPlans.addAll(logicalPlans);
    }

    /**
     * Adds provided physical plans to the list.
     * @param physicalPlans plans to be added
     */
    public void appendPhysicalPlans(ArrayList<PhysicalPlan> physicalPlans) {
        enumeratedPhysicalPlans.addAll(physicalPlans);
    }

    /**
     * @return number of physical plans in the collection
     */
    public int getPhysicalPlanCount() {
        return enumeratedPhysicalPlans.size();
    }

    /**
     * Enumerates all possible physical plans from the provided logical plan.
     * @param logicalPlan logical plan to be turned into physical plans
     * @return collection of physical plans
     */
    public ArrayList<PhysicalPlan> placeLogicalPlan(LogicalPlan logicalPlan, InfrastructurePlan infra) {
        PhysicalPlacementGenerator ppGen = new PhysicalPlacementGenerator();
        // use traditional strategy for pipeline plans
        if (isPipeline(logicalPlan)) {
            return ppGen.generatePhysicalPlans(logicalPlan, infra);
        } else {
            return ppGen.generateBranchedPhysicalPlans(logicalPlan, infra);
        }
    }

    private boolean isPipeline(LogicalPlan logicalPlan) {
        // TODO - get all paths from root to the last operation
        // if more than one path found - the plan is not a pipeline
        return false;
    }

    public ArrayList<PhysicalPlan> getPhysicalPlans() {
        return enumeratedPhysicalPlans;
    }

    public ArrayList<LogicalPlan> getLogicalPlans() {
        return enumeratedLogicalPlans;
    }

    /**
     * Scans through the all physical plans and verifies that all operators
     * can be deployed on each of the selected resources.
     * Non-deployable physical plan is:
     * - a plan that has an operator placed on a resource that doesn't support that operation
     * - that is pretty much it..
     */
    public void pruneNonDeployablePhysicalPlans() {
        ArrayList<PhysicalPlan> deployablePhysicalPlans = new ArrayList<>();
        for (PhysicalPlan enumeratedPhysicalPlan : enumeratedPhysicalPlans) {
            if (isDeployble(enumeratedPhysicalPlan)) {
                deployablePhysicalPlans.add(enumeratedPhysicalPlan);
            }
        }
        enumeratedPhysicalPlans = deployablePhysicalPlans;
    }

    /**
     * Checks whether given plan is deployable.
     */
    private boolean isDeployble(PhysicalPlan physicalPlan) {
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            // get the resource
            InfrastructureNode node = physicalPlan.getOpPlacementNode(op);
            if (!node.canRun(op.getType(), op.getOperator())) {
                return false;
            }

            // bandwidth restrictions
            // is this a edge operator
            if (physicalPlan.getEdgeNodes().contains(op)) {
                // get the data out and compare it with node's RAM
                if (node.getResource().getRam() < op.getDataOut()) {
                    // we cannot deploy this as the RAM on the node is less then the data
                    // that needs to be transmitted to the downstream node
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Finds the cheapest plan from the collection of enumerated plans.
     */
    public PhysicalPlan getExecutionPlan() {
        double minCost = 9.9e10;
        PhysicalPlan execPlan = null;
        for (PhysicalPlan physicalPlan : enumeratedPhysicalPlans) {
            if (physicalPlan.getEnergyCost() < minCost) {
                execPlan = physicalPlan;
                minCost = physicalPlan.getEnergyCost();
            }
        }
        return execPlan;
    }

    /**
     * Finds a single sink operator from the list of provided operators.
     * @param ops list of operators
     * @return final sink operator
     */
    public static CompOperator getSingleSink(ArrayList<CompOperator> ops) {
        //
        for (CompOperator op : ops) {
            if (isLocalSink(op, ops)) {
                return op;
            }
        }

        return null;
    }

    /**
     * Checks whether the operator is a local sink in give list
     */
    private static boolean isLocalSink(CompOperator op, ArrayList<CompOperator> ops) {
        Boolean isSink = true;
        // check if the operator points to any others in the group
        for (Integer downstreamNodeId : op.getDownstreamOpIds()) {
            if (op.getNodeId() == downstreamNodeId) {
                // this is not a sink for this group of operators
                return false;
            }
        }
        return isSink;
    }

    /**
     * Searches through provided compoperators and returns the upstream one
     */
    public static CompOperator getUpstreamOp(CompOperator op, ArrayList<CompOperator> ops) {
        CompOperator upstreamOp = null;
        for (CompOperator tempOp : ops) {
            for (Integer downstreamOpId : tempOp.getDownstreamOpIds()) {
                if (downstreamOpId == op.getNodeId()) {
                    // this is the upstream operator
                    return tempOp;
                }
            }
        }
        return upstreamOp;
    }

    /**
     * Scans plans for windows and applies window safety rules.
     */
    public void applyWinSafetyRules() {
        ArrayList<PhysicalPlan> deployablePhysicalPlans = new ArrayList<>();
        for (PhysicalPlan enumeratedPhysicalPlan : enumeratedPhysicalPlans) {
            Boolean deployable = true;
            CompOperator winOp = null;
            // find the window operator (if present)
            for (CompOperator op : enumeratedPhysicalPlan.getCurrentOps()) {
                if (op.getType().equals("win")) {
                    winOp = op;
                }
            }

            // if window present
            if (winOp != null) {
                // cannot be a source node
                if (enumeratedPhysicalPlan.isSource(winOp)) {
                    logger.debug("[win] Pruning undeployable plan: " + enumeratedPhysicalPlan);
                    deployable = false;
                }

                // all downstream operators must support windows as input
                ArrayList<CompOperator> downNodeOps = enumeratedPhysicalPlan.getDownNodeOps(winOp);
                for (CompOperator downNodeOp : downNodeOps) {
                    InfrastructureNode opPlacementNode = enumeratedPhysicalPlan.getOpPlacementNode(downNodeOp);
                    if (! supportsWin(downNodeOp, opPlacementNode)) {
                        logger.debug("[win] Pruning undeployable plan: " + enumeratedPhysicalPlan);
                        deployable = false;
                    }
                }

            }

            // the plan survived pruning
            if (deployable) {
                deployablePhysicalPlans.add(enumeratedPhysicalPlan);
            }
        }
        enumeratedPhysicalPlans = deployablePhysicalPlans;
    }

    /**
     * Checks whether the operator on this node supports windows
     */
    private boolean supportsWin(CompOperator op, InfrastructureNode node) {
        // find the operator
        for (NodeCapability cap : node.getCapabilities()) {
            if (op.getType().equals(cap.getName())) {
                return cap.supportsWin;
            }
        }

        // no match found
        logger.warn("Capability not found for the operator: " + op.getName());
        return false;
    }

    /**
     * Runs through all physical plans and based on selectivity and generation ratio
     * calculates the data out for all operators.
     * @param eiCoeffs energy impact coefficients
     */
    public void calculateDataOut(EnergyImpactCoefficients eiCoeffs) {
        for (PhysicalPlan physicalPlan : enumeratedPhysicalPlans) {
            // find source operators and calculate the output data payload
            ArrayList<CompOperator> sourceOps = physicalPlan.getSourceOps();

            // update the selectivity and generation ratios
            updateCoefficients(physicalPlan, eiCoeffs);

            for (CompOperator sourceOp : sourceOps) {
                sourceOp.setDataOut(sourceOp.getGenerationRatio() * sourceOp.getSelectivityRatio());
            }

            // calculate all data outputs per window size
            calculateDataOutForAllOps(sourceOps, physicalPlan);
        }
    }

    /**
     * Updates the coefficients the selectivity and generation coefficients
     * for each operator.
     */
    private void updateCoefficients(PhysicalPlan physicalPlan, EnergyImpactCoefficients eiCoeffs) {
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            op.setGenerationRatio(eiCoeffs.getGenerationRatio(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator()));
            op.setSelectivityRatio(eiCoeffs.getSelectivityRatio(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator()));
        }
    }

    /**
     * Loops through the plan and calculatest the data output for all operators
     * @param sourceOps source operators provided
     * @param physicalPlan physical plan
     */
    private void calculateDataOutForAllOps(ArrayList<CompOperator> sourceOps, PhysicalPlan physicalPlan) {
        for (CompOperator sourceOp : sourceOps) {
            // get next operator
            for (Integer downstreamOpId : sourceOp.getDownstreamOpIds()) {
                CompOperator downstreamOp = physicalPlan.getOp(downstreamOpId);

                // if the operator is a window it will buffer up the results
                double winSize = downstreamOp.getType().equals("win") ?
                        Integer.valueOf(downstreamOp.getOperator()) : 1;

                // the data out for this operator is:
                // (upstream op payload * selectivity * generation ratio) * win size
                downstreamOp.setDataOut(Math.round(downstreamOp.getDataOut() + sourceOp.getDataOut() *
                        downstreamOp.getSelectivityRatio() * downstreamOp.getGenerationRatio() * winSize));

                // calc the data out for this operator's downstream operators
                ArrayList<CompOperator> tempOps = new ArrayList<>();
                tempOps.add(downstreamOp);
                calculateDataOutForAllOps(tempOps, physicalPlan);
            }
        }
    }

    /**
     * loops through the plan and calculate per operator data out and triggering frequency
     * @param inputHandler current instance of input handler for operator info
     */
    public void calculateBandwidthTransmissionDataOut(InputHandler inputHandler) {
        for (PhysicalPlan physicalPlan : enumeratedPhysicalPlans) {
            // find source operators and calculate the output data payload
            ArrayList<CompOperator> sourceOps = physicalPlan.getSourceOps();

            // update the selectivity and generation ratios
            updateCoefficients(physicalPlan, inputHandler.getEIcoeffs());

            // calc data out for all operators
            for (CompOperator sourceOp : sourceOps) {
                calcDataOut(sourceOp, physicalPlan.getCurrentOps());
            }

        }
    }

    /**
     * Calculate data out of the operator, 
     * only if all upstream operators have been calculated already 
     * @param op compoperator for which calc needs to happen
     * @param ops all operators within the plan
     */
    private void calcDataOut(CompOperator op, ArrayList<CompOperator> ops) {
        // check that all upstream operators have been calculated (if present in the physical plan exist)
        ArrayList<CompOperator> upstreamOps = getAllUpstreamOp(op, ops);
        double upstreamOpDataOutContribution = 0;
        double maxTriggersPerSecond = -1;
        if (upstreamOps.size() > 0) {
            // check that upstream operators were calculated
            for (CompOperator upstreamOp : upstreamOps) {
                if (!upstreamOp.getComplexDataOut().isCalculated()) {
                    // the upstream operator was not calculated yet, break current operator data out calculation
                    return;
                }
            }

            // all upstream operators are ok - get their dataout contributions
            for (CompOperator upstreamOp : upstreamOps) {
                upstreamOpDataOutContribution += upstreamOp.getComplexDataOut().getEventCountPerTrigger();
                if (maxTriggersPerSecond < op.getGenerationRatio()) {
                    // update generation ratio as to the highest one from the upstream operators
                    maxTriggersPerSecond = op.getGenerationRatio();
                }
            }
        }

        // calc current operator data impact
        if (upstreamOpDataOutContribution > 0) {
            op.getComplexDataOut().setEventCountPerTrigger(upstreamOpDataOutContribution * op.getSelectivityRatio());
        } else {
            op.getComplexDataOut().setEventCountPerTrigger(op.getGenerationRatio());
        }

        // if tumbling window - set selectivity ratio to triggers
        if (!op.getType().toLowerCase().equals("win")) {
            // TODO - check if tumbling window and set to
            op.getComplexDataOut().setTriggersPerSecond(op.getSelectivityRatio());
        } else {
            // update generation/trigger count ratio to the highest
            op.getComplexDataOut().setTriggersPerSecond(maxTriggersPerSecond);
        }

        // get data out for all downstream operators
        for (Integer downstreamOpId : op.getDownstreamOpIds()) {
            CompOperator downstreamOp = getOperator(downstreamOpId, ops);
            calcDataOut(downstreamOp, ops);
        }
        logger.debug(String.format("Op: %s (%s) data out calculated.", op.getType(), op));
    }

    /**
     * Return object of the op from the passed list;
     */
    private CompOperator getOperator(Integer opId, ArrayList<CompOperator> ops) {
        for (CompOperator op : ops) {
            if (op.getNodeId() == opId) {
                return op;
            }
        }
        return null;
    }

    /**
     * Find and return all upstream operators
     */
    private ArrayList<CompOperator> getAllUpstreamOp(CompOperator op, ArrayList<CompOperator> ops) {
        ArrayList<CompOperator> opsOut = new ArrayList<>();
        for (CompOperator tempOp : ops) {
            for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                if (downstreamOpId == op.getNodeId()) {
                    // this is a upstream op - keep it
                    opsOut.add(tempOp);
                }
            }
        }
        return opsOut;
    }


}
