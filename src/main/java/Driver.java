
/**
 * The driver class read the input from command line and run the mapreduce job.
 *
 * Basic Algorithm:
 * 1. Built each line of transition matrix in the mapper and multiplied them with PR matrix in reduce.
 * 2. Using teleporting to solve the spider traps. Default beta 0.15f.
 * 3. use one mapper reduce to find the dead ends contributions
 * 4. Sum up all the unitMultiplication result for PR matrix and deadDead contributions
 * 5. Apply contributions from dead ends to all the nodes.
 *
 * Input:
 * - args0: dir of transition.txt
 * - args1: dir of PageRank.txt
 * - args2: dir of unitMultiplication result
 * - args3: times of convergence
 * - args4: number of total words;
 * - args5: value of beta (Optional: Default 0.15)
 * Output:
 * - uniState: unitMultiplication result
 * - uniStateWithDeadEndSum: unitMultiplication result with DeadEnd PR value collected
 * - prMatrixWithOutDeadEndDistrubution: PR Matrix calculated without DeadEnd sum distributed
 * - prMatrix: Final PR matrix.
 *
 */
public class Driver {

    private static String TEMPDIR_NAME = "tempDir_";

    public static void main(String[] args) throws Exception {


        UnitMultiplication multiplication = new UnitMultiplication();
        FindDeadendsContribution findDeadendsContribution = new FindDeadendsContribution();
        UnitSum sum = new UnitSum();
        ApplyDeadendSum applyDeadendSum = new ApplyDeadendSum();

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        int count = Integer.parseInt(args[2]);
        String websitesCount = args[3];
        String beta = args[4];

        String unitState = TEMPDIR_NAME+"unitState";
        String uniStateWithDeadEndSum = TEMPDIR_NAME+"uniStateWithDeadEndSum";
        String prMatrixWithOutDeadEndDistribution = TEMPDIR_NAME+"prMatrixWithOutDeadEnd";

        for(int i=0; i< count; i++) {
            String[] args1 = {transitionMatrix, prMatrix+i, unitState+i,beta};
            multiplication.main(args1);

            String[] args2 = {transitionMatrix, prMatrix+i,uniStateWithDeadEndSum+i,beta};
            findDeadendsContribution.main(args2);

            String[] args3 = {unitState+i, prMatrix+i,prMatrixWithOutDeadEndDistribution+i,beta,uniStateWithDeadEndSum+i};
            sum.main(args3);

            String[] args4 = {prMatrixWithOutDeadEndDistribution+i, prMatrix+(i+1), websitesCount};
            applyDeadendSum.main(args4);
        }
    }
}
