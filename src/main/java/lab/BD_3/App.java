package lab.BD_3;

import java.text.DecimalFormat;
import java.util.Arrays;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {	
    	DecimalFormat df = new DecimalFormat("#.00");
    	double sum = 1.234567;
    	double a = Double.valueOf(df.format(sum));
    	int b[] = new int[10];
    	b[1] =11;
    	System.out.println(Arrays.toString(b));
    	Arrays.fill(b, -1);
    	System.out.println(Arrays.toString(b));
    }
}
