package Spark;

import net.htmlparser.jericho.Source;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;

public class test {


	
	public static void main(String[] args) {
		Source source=new Source("<html>Hello \n World</html>");
		System.out.println(source.getTextExtractor().setExcludeNonHTMLElements(true).setIncludeAttributes(false).toString());
		
		String data = "Hello there, my name is not importnant right now."
		        + " I am just simple sentecne used to test few things.";
		int maxLenght = 20;
		Pattern p = Pattern.compile("\\G\\s*(.{1,"+maxLenght+"})(?=\\s|$)", Pattern.DOTALL);
		Matcher m = p.matcher(data);
		while (m.find())
		    System.out.println(m.group(1));
	}
}
