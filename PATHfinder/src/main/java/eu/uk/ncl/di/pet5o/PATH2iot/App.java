package eu.uk.ncl.di.pet5o.PATH2iot;

import eu.uk.ncl.di.pet5o.PATH2iot.compile.PathCompiler;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.EplRealm;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost.EnergyImpactEvaluator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.external.PlanWriter;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * PATHfinder is a self-contained module of PATH2iot system.
 *
 * Functionality:
   * EPL decomposition,
   * Non-functional requirements parsing
   * Energy model evaluation
   * Device specific compilation
     * Pebble Watch
     * iPhone / Android
     * Esper node (via d2ESPer)
 *
 * @author Peter Michalak
 *
 * Requires:
   * configuration file (usually input/pathFinder.conf)
 *
 */
public class App 
{
    private static final String PLAN_OUT_FILE = "target/physical_plans.csv";
    private static final boolean DEPLOY = false;
    private static final boolean USE_EXTERNAL_COST_MODULE = true;
    private static Logger logger = LogManager.getLogger(App.class);

    private static InputHandler inputHandler;
    private static NeoHandler neoHandler;
    private static EsperSodaInspector eplInspector;
    private static OperatorHandler opHandler;
    private static PlanWriter planWriter;
    private static InfrastructureHandler infraHandler;
    private static RequirementHandler reqHandler;
    private static SocketClientHandler socketHandler;
    private static ArrayList<EplRealm> eplRealms;

    private static int physicalPlanCounter = 0;
    private static Long startTime = 0L;

    public static void main( String[] args ) {
        // init timer
        startTime = System.currentTimeMillis();

        // 0a parse input files
        inputHandler = new InputHandler(args);

        // 0b init handlers - neo, infra
        initInternalHandlers(inputHandler);

        // 0c check for extended EPL grammar components
        eplInspector.searchExtendedGrammar(inputHandler.getEpls(inputHandler.getEplFile()));

        // iterate through all sets of EPL statements
        eplRealms = new ArrayList<>();
        for (ArrayList<String> epls : eplInspector.getEplBank()) {

            // reset handlers
            initInternalHandlers(inputHandler);

            // 1a decompose EPLs
            eplInspector.parseEpls(epls, inputHandler.getInputStreams(), inputHandler.getUdfs());

            // 1b build graph of infrastructure
            infraHandler = new InfrastructureHandler(inputHandler.getInfrastructureDescription(), neoHandler);

            // 1c build graph of operators - logical plan
            opHandler.buildLogicalPlan(inputHandler.getUdfs());

            // 2a optimise logical plan
            opHandler.appendLogicalPlans(opHandler.applyLogicalOptimisation(opHandler.getInitialLogicalPlan(), "win"));
            logger.info(String.format("[pushing windows] There are %d logical plans.", opHandler.getLogicalPlanCount()));
            // todo push projects closer to the data source
            // todo inject windows

            // 2b enumerate physical plans
            for (LogicalPlan logicalPlan : opHandler.getLogicalPlans()) {
                opHandler.appendPhysicalPlans(opHandler.placeLogicalPlan(logicalPlan,
                        infraHandler.getInfrastructurePlan()));
            }
            physicalPlanCounter += opHandler.getPhysicalPlanCount();
            logger.info(String.format("[generating phys plans] There are %d physical plans in the collection.", opHandler.getPhysicalPlanCount()));

            // 2c calculate data out for all operators in all physical plans
            opHandler.calculateDataOut(inputHandler.getEIcoeffs());

            // 2d prune physical plans
            opHandler.pruneNonDeployablePhysicalPlans();
            opHandler.applyWinSafetyRules();
            logger.info(String.format("[pruning non-deployable plans] There are %d physical plans in the collection.", opHandler.getPhysicalPlanCount()));

            // 3 energy model eval
            EnergyImpactEvaluator eiEval = new EnergyImpactEvaluator(infraHandler.getInfrastructurePlan(),
                    inputHandler.getEIcoeffs());

            // calculate an energy cost for all physical plans
            for (PhysicalPlan physicalPlan : opHandler.getPhysicalPlans()) {
                eiEval.evaluate(physicalPlan);
            }

            // list and persist all plans that comply with the energy requirements
            double energyReq = reqHandler.getRequirement("PebbleWatch", "hour");
            int compliantPlanCount = 0;
            for (PhysicalPlan physicalPlan : opHandler.getPhysicalPlans()) {
                String externalPlanOut = "";
                int planId = eplRealms.size();
                if (USE_EXTERNAL_COST_MODULE) {
                    // prepare the plan for output to external cost model
                    externalPlanOut = planWriter.exportPlan(physicalPlan, planId,
                            eiEval, infraHandler.getInfrastructureNodes());
                }

                // persist all plans that are left after pruning
                eplRealms.add(new EplRealm(eplRealms.size(), physicalPlan, eplInspector,
                        inputHandler.getInfrastructureDescription(), inputHandler.getInputStreams(),
                        externalPlanOut));

                if (physicalPlan.getEstimatedLifetime(inputHandler.getInfrastructureDescription(), "PebbleWatch") > energyReq) {
                    // this is a physical plan that complies with the energy requirements
                    compliantPlanCount++;
                }
            }
            logger.info(String.format("There are %d physical plans that satisfy energy requirements (%s h).",
                    compliantPlanCount, energyReq));

            // return the execution plan based on the cost
            PhysicalPlan executionPlan = opHandler.getExecutionPlan();
            if (executionPlan != null) {
                logger.info(String.format("The cheapest plan is (EI: %.2f):\n%s. Estimated battery life of: %.2f hours",
                        executionPlan.getEnergyCost(), executionPlan,
                        executionPlan.getEstimatedLifetime(inputHandler.getInfrastructureDescription(), "PebbleWatch")));
            } else {
                logger.debug("No plans available.");
            }
        }

        logger.info(String.format("There are %d epl realms stored within memory.", eplRealms.size()));

        EplRealm eplRealmToDeploy = null;
        if (USE_EXTERNAL_COST_MODULE) {
            planWriter.initOutputFile(PLAN_OUT_FILE);
            // export all plans
            for (EplRealm eplRealm : eplRealms) {
                planWriter.writePlan(eplRealm.getPlanOut(), PLAN_OUT_FILE);
            }
            logger.info("All plans exported to: " + PLAN_OUT_FILE);

            // todo receive the answer and pick the best plan
        } else {
            // loop through the realms and find the one with the least cost
            if (eplRealms.size() > 0) {
                eplRealmToDeploy = eplRealms.get(0);
                for (EplRealm eplRealm : eplRealms) {
                    if (eplRealm.getPlan().getEnergyCost() < eplRealmToDeploy.getPlan().getEnergyCost()) {
                        // this is a cheaper plan
                        eplRealmToDeploy = eplRealm;
                    }
                }
            } else {
                logger.info("There are no plans available in any of the eplRealms! Check your constrains.");
            }
        }

        PathCompiler pathCompiler = null;
        if (eplRealmToDeploy != null) {
            logger.info(String.format("The best plan #%d with cost: %.2f (%s)",
                    eplRealmToDeploy.getId(),
                    eplRealmToDeploy.getPlan().getEnergyCost(),
                    eplRealmToDeploy.getPlan()));

            // 4 compile execution plan
            pathCompiler = new PathCompiler();
            pathCompiler.compile(eplRealmToDeploy.getPlan(), eplRealmToDeploy.getEplInsperctor(),
                    eplRealmToDeploy.getInfraDescription(), eplRealmToDeploy.getInputStreams());
            logger.debug(pathCompiler.getExecutionPlan());
        }

        if (DEPLOY && pathCompiler != null) {
            // 5 send the plan to PATHdeployer
            socketHandler = new SocketClientHandler(inputHandler.getPathDeployerIp(), inputHandler.getPathDeployerPort());
            socketHandler.connect();
            socketHandler.send(pathCompiler.getExecutionPlan());
        }

        logger.info("It is done.");
        logger.info(String.format("It took %d ms to process through %d physical plans.",
                (System.currentTimeMillis() - startTime), physicalPlanCounter));
    }


    /**
     * Initialisation of
     * * neo4j handler - establish connection, clean the db
     * * eplInspector - init the ESPer CEP engine
     * * operator handler - logical and physical plan optimisation module
     */
    private static void initInternalHandlers(InputHandler inputHandler) {
        neoHandler = new NeoHandler(inputHandler.getNeoAddress() + ":" + inputHandler.getNeoPort(),
                inputHandler.getNeoUser(), inputHandler.getNeoPass());
        eplInspector = new EsperSodaInspector(neoHandler);
        opHandler = new OperatorHandler(neoHandler);
        reqHandler = new RequirementHandler(inputHandler.getRequirements());
        planWriter = new PlanWriter();
    }
}