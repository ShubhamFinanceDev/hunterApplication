package com.hunter.hunter;

import java.io.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.*;

public class TestXML {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			String file = "F:\\xmldata.xml";
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			// Get the parent node
			Node entreprise = doc.getFirstChild();
			// Get the employee element
			Node employee = doc.getElementsByTagName("employee").item(0);
			// Get the list of child nodes of employee
			NodeList list = employee.getChildNodes();
			for (int i = 0; i < list.getLength(); i++) {
				Node node = list.item(i);
				// Remove "name" node
				if ("name".equals(node.getNodeName())) {
					//employee.removeChild(node);
				}
			}

			//System.out.println(doc.getElementsByTagName("entreprise").item(0).getChildNodes().toString());
			// write the content to the xml file
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			DOMSource src = new DOMSource(doc);
			StreamResult res = new StreamResult(new File(file));
			transformer.transform(src, res);

			String str = "<root><entreprise>\r\n" + 
					"   <employee id=\"1\">\r\n" + 
					"      \r\n" + 
					"      <age>25</age>\r\n" + 
					"      <address>San Francisco</address>\r\n" + 
					"   </employee>\r\n" + 
					"</entreprise></root>";

			String str2 = str.replace("<root>", "").replace("</root>", "");
			System.out.println(str2);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
