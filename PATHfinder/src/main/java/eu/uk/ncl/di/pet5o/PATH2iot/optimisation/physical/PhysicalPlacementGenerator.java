package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Generates a physical placements based on passed parameters.
 *
 * @author Peter Michalak
 */
public class PhysicalPlacementGenerator {

    private static Logger logger = LogManager.getLogger(PhysicalPlacementGenerator.class);

    public PhysicalPlacementGenerator() {}

    /**
     * Takes in a logical plan and description of computation and enumerates all possible deployemnt
     * options for given plan
     * @param logPlan logical plan to be placed
     * @param infra current active infrastructure
     * @return collection of physical plans
     */
    public ArrayList<PhysicalPlan> generatePhysicalPlans(LogicalPlan logPlan, InfrastructurePlan infra) {
        // create initial plan(s) - first operator(s) placed on all infrastructure nodes
        ArrayList<PhysicalPlan> ppCatalogue = new ArrayList<>();

        // for each first operator in a logical plan
        for (CompOperator op : logPlan.getFirstOperators()) {

            // place this operator on a node (assuming computation can start anywhere)
            // the un-deployable plans will be pruned in the later step
            for (InfrastructureNode node : infra.getNodes()) {
                PhysicalPlan newPhysPlan = new PhysicalPlan(logPlan);
                newPhysPlan.place(op, node, infra);

                // validate that the plan is complete
                if (newPhysPlan.isComplete()) {
                    ppCatalogue.add(newPhysPlan);
                } else {
                    placeRemainingOperators(newPhysPlan, infra, ppCatalogue);
                }
            }
        }

        return ppCatalogue;
    }

    /**
     * Another take on placing all operators on available infrastructure.
     */
    private void placeRemainingOperators(PhysicalPlan physPlan, InfrastructurePlan infra, ArrayList<PhysicalPlan> ppCatalogue) {
        // get remaining operators
        CompOperator unplacedOp = physPlan.getUnplacedOperators();

        // get placement nodes
        List<InfrastructureNode> placementNodes = physPlan.getPlacementNodes(unplacedOp, infra);

        for (InfrastructureNode placementNode : placementNodes) {
            // place remaining operator
            PhysicalPlan newPhysPlan = physPlan.getCopy();

            // check that the physical plan is complete
            newPhysPlan.place(unplacedOp, placementNode, infra);

            if (newPhysPlan.isComplete()) {
                ppCatalogue.add(newPhysPlan);
            } else {
                placeRemainingOperators(newPhysPlan, infra, ppCatalogue);
            }
        }
    }

    /**
     * Given the logical plan is branched - following approach is used
     *   - represent each operator in a vector
     *   - create all permutations of the vector given number of infrastructures
     *     placing each operator exhaustively on each node within the infrastructure
     * @param logicalPlan logical plan to be enumerated
     * @param infra current infrastructure
     * @return all placement possibilities
     */
    public ArrayList<PhysicalPlan> generateBranchedPhysicalPlans(LogicalPlan logicalPlan, InfrastructurePlan infra) {
        int numberOfOperators = logicalPlan.getOpCount();
        int numberOfPlacementPossibilities = infra.getNodes().size();
        ArrayList<Long> deployablePlanIds = new ArrayList<>();
        ArrayList<PhysicalPlan> physicalPlans = new ArrayList<>();

        // if only two platforms are present go through binary placement
        if (numberOfPlacementPossibilities == 2) {
            logger.debug("Start enumerating plans: " + System.currentTimeMillis());
            long numberOfPlans = (long) Math.pow(numberOfPlacementPossibilities, numberOfOperators);
            long deployablePlanCount = 0;
            for (long i = 0; i < numberOfPlans; i ++) {
                if (isBranchedDeployable(i, logicalPlan, infra)) {
                    if(flowsOnlyDownstream(i, logicalPlan)) {
                        deployablePlanIds.add(i);
                        break;
                    }
                }
                if (i % 25000000 == 0) {
//                    deployablePlanCount += deployablePlanIds.size();
                    logger.debug(String.format("%d still processing found new: %d total: %d out of %d plans (%.2f%%)", System.currentTimeMillis(),
                            deployablePlanIds.size(), deployablePlanCount, numberOfPlans, (double) i / numberOfPlans * 100));
                }
            }
            logger.debug("Remaining deployable plans: " + deployablePlanIds.size() + " @" + System.currentTimeMillis());

            // map to deployable plan
            for (Long planId : deployablePlanIds) {
                // take id // take logical plan and create a physical one
                PhysicalPlan tempPhysPlan = new PhysicalPlan(logicalPlan);
                for (int i = 0; i < logicalPlan.getOperators().size(); i++) {
                    // select infrastructure node based on plan id placement
                    InfrastructureNode tempNode =
                            ((planId >> i) & 1) == 1 ? infra.getNodes().get(1) : infra.getNodes().get(0);
//                    CompOperator op = tempPhysPlan.getOp(logicalPlan.getOperators().get(i).getNodeId());
                    tempPhysPlan.directPlacement(logicalPlan.getOperators().get(i), tempNode);
                }
                physicalPlans.add(tempPhysPlan);
            }
//            try {
//                Files.write(Paths.get(String.format("out/pp-id_%d.txt", System.currentTimeMillis())),
//                        Collections.singleton(deployablePlanIds.toString()));
//            } catch (IOException e) {
//                logger.error("Unable to write the plans to a file", e);
//            }
        } else {
            logger.error("Branched physical generator doesn't support more than 2 platforms!");
        }

        return physicalPlans;
    }

    /**
     * Check whether the physical plan only flows downstream
     * @param planId id of a current plan that serves as placement for binary option of physical placement 0/1
     * @param logicalPlan logical plan definition
     * @return true only if all operators transmit data either on the same platform or a downstream one
     */
    private boolean flowsOnlyDownstream(long planId, LogicalPlan logicalPlan) {
        for (int i = 0; i < logicalPlan.getOperators().size(); i++) {
            // if current operator has a downstream operator - check that there are placed on the same platform
            // or downstream operator is placed only on a downstream platform
            CompOperator op = logicalPlan.getOperators().get(i);
            if (op.getDownstreamOpIds().size() == 0)
                continue;

            // this operator has downstream operators - get placement
            int opPlacement = (int) ((planId >> i) & 1);

            // get all downstream operators
            for (Integer downstreamOpId : op.getDownstreamOpIds()) {
               for(int j = 0; j < logicalPlan.getOperators().size(); j++) {
                   if (downstreamOpId == logicalPlan.getOperators().get(j).getNodeId()) {
                       // this is the order of the downstream operator within the logical plan
                       int downstreamOpPlacement = (int) ((planId >> j) & 1);

                       // check that there are placed together or downstream
                       if (opPlacement > downstreamOpPlacement)
                           return false;
                       break;
                   }
               }
            }
        }
        return true;
    }

    /**
     * Checks capabilities of infrastructure
     * @param planId id of a current plan that serves as placement for binary option of physical placement 0/1
     * @param logicalPlan logical plan definition
     * @param infra description of infrastructure with its capabilities
     * @return is deployable
     */
    private boolean isBranchedDeployable(long planId, LogicalPlan logicalPlan, InfrastructurePlan infra) {
        ArrayList<CompOperator> ops = logicalPlan.getOperators();
        for (int i = 0; i < ops.size(); i++) {
            // get placement node
            InfrastructureNode node = ((planId >> i) & 1) == 0 ? infra.getNodes().get(0) : infra.getNodes().get(1);
            if (!node.canRun(ops.get(i).getType(), ops.get(i).getOperator()))
                return false;
        }
        return true;
    }

}