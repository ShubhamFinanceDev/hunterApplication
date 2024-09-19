package com.hunter.hunter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.persistence.TupleElement;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
//@EnableScheduling
public class SpringConfig {

	List<String> headerValues = new ArrayList<String>();
	@Autowired
	private EntityManager entityManager;
	@Autowired
	@Qualifier("jdbcTemplate2")
	private JdbcTemplate osourceTemplate;

	private static int PARAMETER_LIMIT = 999;

	//@Scheduled(fixedDelay = 40000)
	public void scheduleFixedDelayTask() {
		System.out.println("Fixed delay task - " + System.currentTimeMillis() / 1000);
		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet sheet = workbook.createSheet("lawix10");

		Query q = entityManager.createNativeQuery(
				"select app.\"Application Number\",case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'A' end||'_'||app.\"Application Number\"||'_'||'HOU'  IDENTIFIER,app.\"Product Type Code\" PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,app.\"Branch Code\" BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"`Branch`\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Sanction Date\" between sysdate-4  and sysdate and app.\"Referral Code\" is not null and rownum<10",
				Tuple.class);

		/*
		 * List<Tuple> results = q.getResultList(); try { if (results != null &&
		 * results.size() > 0) { writeHeaderLine(results, sheet);
		 * 
		 * writeDataLines(results, workbook, sheet); String yemi = "test.xlsx";
		 * FileOutputStream fileOut = new FileOutputStream(yemi);
		 * workbook.write(fileOut); fileOut.close(); }
		 * 
		 * System.out.println("file generated successfully"); } catch (SQLException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } catch
		 * (FileNotFoundException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (IOException e) { // TODO Auto-generated catch
		 * block e.printStackTrace(); }
		 */
	}

	private void writeHeaderLine(List<Tuple> results, XSSFSheet sheet) throws SQLException {

		Row headerRow = sheet.createRow(0);

		Tuple t = results.get(0);
		List<TupleElement<?>> cols = t.getElements();
		System.out.println(cols.size());
		for (int i = 0; i < cols.size(); i++) {
			if (cols.get(i) != null && cols.get(i).getAlias() != null && t.get(cols.get(i).getAlias()) != null) {

				String headerVal = cols.get(i).getAlias().toString();
				Cell headerCell = headerRow.createCell(i);
				headerCell.setCellValue(headerVal);
				headerValues.add(headerVal);
			}
		}

	}

	private void writeDataLines(List<Tuple> results, XSSFWorkbook workbook, XSSFSheet sheet) throws SQLException {
		int rowCount = 1;

		for (Tuple t : results) {
			Row row = sheet.createRow(rowCount++);

			List<TupleElement<?>> cols = t.getElements();

			for (int p = 0; p < headerValues.size(); p++) {
				row.createCell((short) p).setCellValue(t.get(headerValues.get(p)).toString());
			}

		}

	}
}
