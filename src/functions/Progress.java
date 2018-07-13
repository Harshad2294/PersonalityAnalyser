package functions;
/**
 * @author Mike Shauneu
 * @link https://stackoverflow.com/questions/4573123/java-updating-text-in-the-command-line-without-a-new-line
 * 
 * extended by Harshad Shettigar
 */
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class Progress {

	private int totalRows;
	private long startTime;
	private int counter;
	private Thread progress;
	private static final String SPACE="\r                                                                           ";

	public Progress(){
	}

	public Progress(int totalRows) {
		this.totalRows=totalRows;
		this.startTime=System.currentTimeMillis();
		counter = 0;
	}
	
	public void updateTasks(int totalRows){
		this.totalRows=totalRows;
		this.startTime=System.currentTimeMillis();
		this.counter=0;
	}

	public void update(){
		counter++;
		printProgress(startTime, totalRows, counter);	
	}

	public void printProgress(String function){
		progress = new ShowProgress(function);
		progress.start();
	}

	public void stopProgress() {
		try {
			progress.interrupt();
		} catch (Exception e) {}
	}

	private void printProgress(long startTime, long total, long current) {
		long eta = current == 0 ? 0 : 
			(total - current) * (System.currentTimeMillis() - startTime) / current;
		String etaHms = current == 0 ? "N/A" : 
			String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
					TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
					TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));
		StringBuilder string = new StringBuilder(140);
		int percent = (int) (current * 50 / total);
		string
		.append('\r')
		.append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
		.append(String.format(" %d%% [", percent*2))
		.append(String.join("", Collections.nCopies(percent, "=")))
		.append('>')
		.append(String.join("", Collections.nCopies(50 - percent, " ")))
		.append(']')
		.append(String.join("", Collections.nCopies((int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
		.append(String.format(" %d/%d, ETA: %s", current, total, etaHms));
		System.out.print(string);
	}

	class ShowProgress extends Thread
	{
		private String function;
		private int dots;
		public ShowProgress(String function) {
			this.function=function;
			this.dots=0;
		}
		@Override
		public void run() {
			try {
				while(true)
				{
					if(dots==5)
					{
						dots=0;
						System.out.print(SPACE);
					}
					String temp="";
					for(int i=0;i<dots;i++) temp+=".";
					System.out.print("\r"+function+temp);
					Thread.sleep(500);
					dots++;
				}
			} catch (InterruptedException e) {}
		}
		
		@Override
		public void interrupt() {
			System.out.println("\r"+function+": DONE");
			super.interrupt();
		}
	}
}
