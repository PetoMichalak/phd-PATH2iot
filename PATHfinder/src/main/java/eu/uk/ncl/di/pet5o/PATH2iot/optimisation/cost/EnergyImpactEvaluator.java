package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.energy.EnergyImpactCoefficients;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

/**
  * Calculates cost of current energy model
  *   EI = ~ OS_{idle} + \sum_{i}^{n}{comp\_cost_{i}} +
  *          \frac{msg\_count*net\_cost + BLE_{active} * BLE_{duration}}{cycle\_length}
  *
  * @author Peter Michalak
 */
public class EnergyImpactEvaluator {

    private static Logger logger = LogManager.getLogger(EnergyImpactEvaluator.class);

    private InfrastructurePlan infraPlan;
    private EnergyImpactCoefficients eiCoeffs;

    public EnergyImpactEvaluator(InfrastructurePlan infra, EnergyImpactCoefficients eiCoeffs) {
        this.infraPlan = infra;
        this.eiCoeffs = eiCoeffs;
    }

    /**
     * Evaluates the energy cost of physical plan, based on the EI coefficients.
     * @param physicalPlan plan to be evaluated
     * @return the overall energy cost for given physical plan
     */
    public double evaluate(PhysicalPlan physicalPlan) {
        // I. computation impact
        double compCost = 0;
        // add cost for running on each platform (OSidle)
        for (InfrastructureNode node : physicalPlan.getAllNodes()) {
            compCost += eiCoeffs.getCost(node.getResourceType(), "OSidle", "");
        }

        // for each node calc impact of the computation
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            compCost += eiCoeffs.getCost(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator());
        }

        // identify the data transfer points and calculate the networking impact based on EI formulae
        // II. networking impact
        double networkCost = 0;
        for (CompOperator op : physicalPlan.getEdgeNodes()) {
            // for each downstream operator
            for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                CompOperator downstreamOp = physicalPlan.getOp(downstreamOpId);
                InfrastructureNode opNode = physicalPlan.getOpPlacementNode(op);
                InfrastructureNode downstreamOpNode = physicalPlan.getOpPlacementNode(downstreamOp);

                // # msgs = ceil(payload / bandwidth)
                int numberOfMessages = (int) Math.ceil(op.getDataOut() /
                        infraPlan.getBandwidthLimit(opNode, downstreamOpNode));

                // calc the netCost
                // msg_count * net_cost_thisOP + msg_count * net_cost_downstream_op
                double opNetworkCost = numberOfMessages * eiCoeffs.getCost(opNode.getResourceType(), "netCost", "") +
                        numberOfMessages * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "netCost", "");

                // calc the bleActive - default is 1s
                // BLEduration * bleActive
                int bleActive = 1;
                int winLen = 1;
                if (op.getType().equals("win")) {
                    // this one's tricky - if the window is larger than 10 seconds - the BLEduration is 10
                    // if the window is shorter than the window is win - 1 (minus one)
                    winLen = Integer.valueOf(op.getOperator());
                    bleActive = winLen > 10 ? 10 : winLen - 1;
                } else {
                    // a default streaming frequency will be used as no window is present - recalculate network cost
                    InfrastructureNode placementNode = physicalPlan.getOpPlacementNode(op);
                    opNetworkCost = placementNode.getDefaultNetworkFreq() * eiCoeffs.getCost(opNode.getResourceType(), "netCost", "") +
                            numberOfMessages * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "netCost", "");
                }
                opNetworkCost +=  bleActive * eiCoeffs.getCost(opNode.getResourceType(), "bleActive", "") +
                        bleActive * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "bleActive", "");

                // normalise to the cycle 1 s
                networkCost += opNetworkCost / winLen;
            }
        }

        // set the energy cost
        physicalPlan.setEnergyCost(compCost + networkCost);

        return physicalPlan.getEnergyCost();
    }

    /**
     * Calculates Energy Impact of all operators running on the node
     * @param physicalPlan physical plan that should be calculated
     * @param node node that is being evaluated
     * @return energy impact cost of all operators on supplied node
     */
    public double evaluateNodeCost(PhysicalPlan physicalPlan, InfrastructureNode node) {
        ArrayList<CompOperator> opsOnNode = physicalPlan.getOpsOnNode(node);

        double compCost = 0;
        // get the computation cost of running anything on the platform
        compCost += eiCoeffs.getCost(node.getResourceType(), "OSidle", "");
        for (CompOperator op : opsOnNode) {
            // get EI of operation
            compCost += eiCoeffs.getCost(node.getResourceType(), op.getType(), op.getOperator());
        }

        double netCost = 0;
        for (CompOperator op : physicalPlan.getEdgeNodes()) {
            // if op is on the node -> add the network cost
            if (physicalPlan.getOpPlacementNode(op) == node) {
                // for each downstream operator
                for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                    CompOperator downstreamOp = physicalPlan.getOp(downstreamOpId);
                    InfrastructureNode opNode = physicalPlan.getOpPlacementNode(op);
                    InfrastructureNode downstreamOpNode = physicalPlan.getOpPlacementNode(downstreamOp);

                    // # msgs = ceil(payload / bandwidth)
                    int numberOfMessages = (int) Math.ceil(op.getDataOut() /
                            infraPlan.getBandwidthLimit(opNode, downstreamOpNode));

                    // calc the netCost
                    // msg_count * net_cost_thisOP + msg_count * net_cost_downstream_op
                    double opNetworkCost = numberOfMessages * eiCoeffs.getCost(opNode.getResourceType(), "netCost", "") +
                            numberOfMessages * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "netCost", "");

                    // calc the bleActive - default is 1s
                    // BLEduration * bleActive
                    int bleActive = 1;
                    int winLen = 1;
                    if (op.getType().equals("win")) {
                        // this one's tricky - if the window is larger than 10 seconds - the BLEduration is 10
                        // if the window is shorter than the window is win - 1 (minus one)
                        winLen = Integer.valueOf(op.getOperator());
                        bleActive = winLen > 10 ? 10 : winLen - 1;
                    } else {
                        // a default streaming frequency will be used as no window is present - recalculate network cost
                        InfrastructureNode placementNode = physicalPlan.getOpPlacementNode(op);
                        opNetworkCost = placementNode.getDefaultNetworkFreq() * eiCoeffs.getCost(opNode.getResourceType(), "netCost", "") +
                                numberOfMessages * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "netCost", "");
                    }
                    opNetworkCost += bleActive * eiCoeffs.getCost(opNode.getResourceType(), "bleActive", "") +
                            bleActive * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "bleActive", "");

                    // normalise to the cycle 1 s
                    netCost += opNetworkCost / winLen;
                }
            }
        }


        return compCost + netCost;
    }
}
