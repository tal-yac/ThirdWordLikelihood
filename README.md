# ThirdWordLikelihood
### BGU DSPS1 212 assignment 2
Calculates the likelihood of a word to appear after 2 given words

How to run:	0. make sure the config and credentials files are configured properly inside the .aws folder, there are no hard coded defaults for security reasons.
		        1. compile with mvn clean install.
	     	    2. move the jar inside the target folder with the postfix "with-dependencies to jar folder"
            3. rename the copy to "steps.jar".
	     	    4. run with "java -jar yourjar.jar".

step1: input is heb-3-gram, counts all occurances and joins w1w2w3 occurances with w3 occurances.
step2: joins w1w2w3 occurances with w2 and w2w3 occurances.
step3: joins w1w2w3 occurances with w1w2 and calculates the likelihood.
step4: sorts w1w2w3 by w1w2 ascending and likelihood descending.

All steps are scalable with O(1) space using sorting and partioning.
