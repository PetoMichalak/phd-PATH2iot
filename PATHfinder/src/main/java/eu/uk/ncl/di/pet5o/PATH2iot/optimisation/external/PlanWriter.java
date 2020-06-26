package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.external;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost.EnergyImpactEvaluator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.OperatorHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PlanWriter {

    private static Logger logger = LogManager.getLogger(PlanWriter.class);

    /**
     * Writes plans to a csv file for external optimiser module.
     * @param opHandler Operator handler, that contains all physical plans
     * @param eiEval initialised energy impact evaluator
     * @param infraNodes infrastructure nodes that are present in the environment
     * @param outFile output file for the plans to be written to
     */
    public void write(OperatorHandler opHandler, EnergyImpactEvaluator eiEval,
                      ArrayList<InfrastructureNode> infraNodes, String outFile) {
        int planCounter = 0;
        List<String> outBuffer = new ArrayList<>();
        // give the output buffer a header
        outBuffer.add("id,total_ei,iot_ei,edge_ei,cloud_ei,iot_data_out,edge_data_out,cloud_data_out,iot_win,edge_win,cloud_win");
        for (PhysicalPlan physicalPlan : opHandler.getPhysicalPlans()) {
            logger.debug(String.format("%.2f EI: %s", physicalPlan.getEnergyCost(), physicalPlan));
            String out = String.format("%d,%.4f", planCounter, physicalPlan.getEnergyCost());

            // iterate through all of the nodes and add the partial ei cost for
            // all operations on that node
            for (InfrastructureNode node : infraNodes) {
                out = String.format("%s,%.2f", out, eiEval.evaluateNodeCost(physicalPlan, node));
            }

            // add data output per each device
            for (InfrastructureNode node : infraNodes) {
                out = String.format("%s,%.2f", out, physicalPlan.getDataOutput(node));
            }

            // add window information per each node
            for (InfrastructureNode node : infraNodes) {
                out = String.format("%s,%.3f", out, physicalPlan.getWinLen(node));
            }

            outBuffer.add(out);
            planCounter++;
        }

        // save to a file for external cost model
        Path file = Paths.get(outFile);
        try {
            Files.write(file, outBuffer, Charset.forName("UTF-8"));
        } catch (IOException e) {
            logger.error("Couldn't save the file " + outFile + ":" + e.getMessage());
        }
    }


    /**
     * Exports physical plan to external format
     * @param physicalPlan plan to be exported
     * @param planId id of the plan
     * @param eiEval initialised energy impact evaluator
     * @param infraNodes infrastructure nodes that are present in the environment
     * @return text output of the current plan in format:
     *         id,total_ei,iot_ei,edge_ei,cloud_ei,iot_data_out,edge_data_out,cloud_data_out,iot_win,edge_win,cloud_win
     */
    public String exportPlan(PhysicalPlan physicalPlan, int planId, EnergyImpactEvaluator eiEval, ArrayList<InfrastructureNode> infraNodes) {
        logger.debug(String.format("%.2f EI: %s", physicalPlan.getEnergyCost(), physicalPlan));
        String out = String.format("%d,%.4f", planId, physicalPlan.getEnergyCost());

        // iterate through all of the nodes and add the partial ei cost for
        // all operations on that node
        for (InfrastructureNode node : infraNodes) {
            out = String.format("%s,%.2f", out, eiEval.evaluateNodeCost(physicalPlan, node));
        }

        // add data output per each device
        for (InfrastructureNode node : infraNodes) {
            out = String.format("%s,%.2f", out, physicalPlan.getDataOutput(node));
        }

        // add window information per each node
        for (InfrastructureNode node : infraNodes) {
            out = String.format("%s,%.3f", out, physicalPlan.getWinLen(node));
        }
        return out;
    }

    /**
     * Writes a header information to the external file for the output.
     */
    public void initOutputFile(String filePath) {
        String header = "id,total_ei,iot_ei,edge_ei,cloud_ei,iot_data_out,edge_data_out,cloud_data_out,iot_win,edge_win,cloud_win";
        // write header to the output file
        try {
            Files.write(Paths.get(filePath), (header + System.lineSeparator()).getBytes(UTF_8));
        } catch (IOException e) {
            logger.error("Couldn't save the file " + filePath + ":" + e.getMessage());
        }
    }

    /**
     * Writes a plan to an output file (APPEND)
     * @param planOut plan that should be written
     * @param filePath path to the output file
     */
    public void writePlan(String planOut, String filePath) {
        // write header to the output file
        try {
            Files.write(Paths.get(filePath), (planOut + System.lineSeparator()).getBytes(UTF_8),
                    StandardOpenOption.CREATE,StandardOpenOption.APPEND);
        } catch (IOException e) {
            logger.error("Couldn't save the file " + filePath + ":" + e.getMessage());
        }
    }
}
