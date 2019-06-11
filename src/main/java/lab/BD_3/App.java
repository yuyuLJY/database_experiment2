package lab.BD_3;

import java.text.DecimalFormat;

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
        System.out.println(a);
    }
}
