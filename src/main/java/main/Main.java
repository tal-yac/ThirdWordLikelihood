package main;

public class Main {

	public static void main(String[] args) throws Exception {
		if (args.length == 0)
			start.Main.main(args);
		else if (args[0].equals("1"))
			step1.Main.main(args);
		else if (args[0].equals("2"))
			step2.Main.main(args);
		else if (args[0].equals("3"))
			step3.Main.main(args);
		else if (args[0].equals("4"))
			step4.Main.main(args);
	}

}
