# ThirdWordLikelihood
### BGU DSPS1 212 Assignment 2
Calculates the likelihood of a word to appear after 2 given words

The assignment is done using Hadoop's Map-Reduce and running over Amazon's EMR.

How to run:
1. make sure the config and credentials files are configured properly inside the .aws folder, there are no hard coded defaults for security reasons.
2. compile with mvn clean install.
3. move the jar inside the target folder with the postfix "with-dependencies to jar folder"
4. rename the copy to "steps.jar".
5. run with "java -jar yourjar.jar".

Steps Description:
- Step 1: input is heb-3-gram, counts all occurances and joins w1w2w3 occurances with w3 occurances.
- Step 2: joins w1w2w3 occurances with w2 and w2w3 occurances.
- Step 3: joins w1w2w3 occurances with w1w2 and calculates the likelihood.
- Step 4: sorts w1w2w3 by w1w2 ascending and likelihood descending.

All steps are scalable with only O(1) extra space using sorting and partioning.
