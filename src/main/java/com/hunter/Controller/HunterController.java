package com.hunter.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.mail.internet.MimeMessage;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.Tuple;
import javax.persistence.TupleElement;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.collect.Lists;
import com.hunter.hunter.Email;
import com.hunter.hunter.QrtzEmailConfig;

@RestController
public class HunterController {

	@Autowired
	private EntityManager entityManager;

	@Autowired
	private JavaMailSender javaMailSender;

	@Autowired
	@Qualifier("jdbcTemplate2")
	private JdbcTemplate osourceTemplate;

	@SuppressWarnings("unused")
	private static int PARAMETER_LIMIT = 999;
	private List<String> headerValues = new ArrayList<String>();

	@RequestMapping(value = "/downloadDataFile/{filename:.+}")
	public void getLogFile(@PathVariable("filename") String filename, HttpSession session, HttpServletResponse response)
			throws Exception {
		try {
			InputStream inputStream = new FileInputStream(new File(filename));
			response.setContentType("application/octet-stream");
			response.setHeader("Content-Disposition", "attachment; filename=" + filename);
			IOUtils.copy(inputStream, response.getOutputStream());
			response.flushBuffer();
			inputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	////////////////////////////////////////////////ADHOC DATA///////and app.\"Sanction Date\" is not null//////////////////////////////////////////////////
	@GetMapping("/adhocfetchData")
	public List<ObjectNode> adhocfetchData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME", "MA_LST_NME", "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD",
				"MA_PA_CTY", "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY", "MA_RA_STE",
				"MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY", "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN",
				"MA_HT_TEL_NO", "MA_M_TEL_NO", "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
				"MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP", "MA2_DOC_NO", "MA3_DOC_TYP",
				"MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO", "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO",
				"MA7_DOC_TYP", "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO", "MA_ORG_NME",
				"MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD", "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY",
				"MA_EMP_PIN", "MA_EMP_TEL", "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
				"MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB", "JA_AGE", "JA_GNDR",
				"JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME", "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1",
				"JA2_PAN", "JA2_FST_NME", "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
				"JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD", "JA1_RA_CTY", "JA1_RA_STE",
				"JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD", "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN",
				"JA_RA_DOC_TYP_1", "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
				"JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5", "JA_RA_DOC_NO_5",
				"JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7", "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8",
				"JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9", "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10",
				"JA1_RA_DOC_TYP_1", "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
				"JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5", "JA1_RA_DOC_NO_5",
				"JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7", "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8",
				"JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9", "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
				"JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2", "JA2_RA_DOC_TYP_3",
				"JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4", "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5",
				"JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6", "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8",
				"JA2_RA_DOC_NO_8", "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10", "JA2_RA_DOC_NO_10",
				"RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME", "RF2_FST_NME", "RF2_LST_NME", "RF_ADD",
				"RF_CTY", "RF_STE", "RF_CTRY", "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN",
				"RF2_ADD", "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO", "RF2_TEL_NO",
				"RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME", "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE",
				"BR_CTRY", "BR_PIN" };
		ObjectMapper mapper = new ObjectMapper();
		List<ObjectNode> json = new ArrayList<>();
		SimpleDateFormat querydate = new SimpleDateFormat("DD-MMM-YY");
		SimpleDateFormat formatDate = new SimpleDateFormat("hh:mm a");
		String hour = formatDate.format(new Date());
		System.out.println(hour);
		String querysubmission = null;
		Set<String> appList = new HashSet<>();
		List<Tuple> fetchapplication = new ArrayList<>();
		List<Tuple> mainApplicantAll = new ArrayList<>();
		List<Tuple> mainApplicantRAAll = new ArrayList<>();
		List<Tuple> mainApplicantPAAll = new ArrayList<>();
		List<Tuple> mainApplicantMobileAll = new ArrayList<>();
		List<Tuple> mainApplicantBankAll = new ArrayList<>();
		List<Tuple> mainApplicantEmployerAll = new ArrayList<>();
		List<Tuple> jointApplicantAll = new ArrayList<>();
		List<Tuple> jointApplicantRAAll = new ArrayList<>();
		List<Tuple> brokerAll = new ArrayList<>();
		List<Tuple> brokerAddressAll = new ArrayList<>();
		List<Tuple> referenceApplicantAll = new ArrayList<>();
		List<Tuple> referenceApplicantRAAll = new ArrayList<>();
		List<Tuple> referencetApplicantMobileAll = new ArrayList<>();
		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet huntersheet = workbook.createSheet("HUNTER");
		Row headerRow = huntersheet.createRow(0);
		List<String> cols = Arrays.asList(headerArray);
		System.out.println(cols.size());
		for (int i = 0; i < cols.size(); i++) {
			String headerVal = cols.get(i).toString();
			Cell headerCell = headerRow.createCell(i);
			headerCell.setCellValue(headerVal);

		}

		try {
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(CURDATE(),'%d-%b-%y') todate,DATE_FORMAT(date(CURDATE()-1),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;
			
				querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,case when app.\"Product Type Code\" like 'HL' then 'HL_IND' else 'NHL_IND' end PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Preet Vihar' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='BHOPAL' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='SANGLI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='MEERUT' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='WARDHA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Naroda' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDNAGAR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JHANSI' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='AKOLA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='GWALIOR' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='MATHURA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='DHULE' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='SAGAR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='JAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='RAIPUR' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='NAGPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='BOKARO' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRAVATI' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='PATNA' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='Rewari' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='Vasai' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILAI' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='MADANGIR' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='SHRIRAMPUR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='BULDHANA' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SATARA' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KALYAN' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='FARIDABAD' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='INDORE' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILWARA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='Sonipat' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='PANIPAT' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='MANGOLPURI' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMSHEDPUR' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='JABALPUR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='MUZAFFARPUR' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='GURGAON' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEHRADUN' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='Patiala' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='UDAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='YAVATMAL' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='VARANASI' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PUNE' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KANPUR' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOTA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ALLAHABAD' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PARBHANI' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='RAJKOT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='PIMPRI CHINWAD' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='LUCKNOW' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='LATUR' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SURAT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALANDHAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='AJMER' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='LUDHIANA' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='CHANDRAPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Bhatinda' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='KARNAL' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='BELAPUR' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='SAHARANPUR' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='BAREILLY' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOLHAPUR' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='AURANGABAD' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAGATPURA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ROORKEE' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='VADODARA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='RANCHI' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMBALA' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='AGRA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='NASHIK' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMNAGAR' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JODHPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='BARAMATI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='Chandan Nagar' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='NANDED' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='JUNAGADH' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Janakpuri' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRITSAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='Noida' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEWAS' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='WAPI' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALGAON' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='UJJAIN' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDABAD' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Rohtak' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='Panvel' THEN 'Mumbai'\r\n" + "ELSE  br.\"Branch Name\"\r\n"
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null  and app.\"Product Type Code\" is not null and app.\"Application Number\" in ('APPL05192235','APPL05192240','APPL05192314','APPL05192622','APPL05193179','APPL05189230','APPL05191120','APPL05191360','APPL05191334','APPL05183394') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\") ";

			

				q = entityManager.createNativeQuery(querysubmission, Tuple.class);

			fetchapplication = q.getResultList();

			json = _toJson(fetchapplication);
			int rowCount = 1;
			for (int app = 0; app < json.size(); app++) {

				Row row = huntersheet.createRow(rowCount++);
				for (int p = 0; p < cols.size(); p++) {
					if (json.get(app).get(cols.get(p)) != null) {
						row.createCell((short) p).setCellValue(json.get(app).get(cols.get(p)).asText());
					}
				}
				String appNo = json.get(app).get("Application Number").asText();
				appList.add(appNo);
				String custNo = json.get(app).get("Customer Number").asText();
				System.out.println(appNo + "                    " + custNo);

				String neoCIFID = null;
				if (json.get(app).has("Neo CIF ID")) {
					neoCIFID = json.get(app).get("Neo CIF ID").asText();
				}
				System.out.println(neoCIFID + "                    " + neoCIFID);

				// Main Application Data Population End
				q = entityManager.createNativeQuery(
						"select \"Application Number\", case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'\r\n"
								+ " and \"Identification Type\" is not null and \"Application Number\" in ('" + appNo
								+ "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				if (mainApplicant != null && !mainApplicant.isEmpty()) {
					mainApplicantAll.addAll(mainApplicant);

					List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
					for (int mainapp = 0; mainapp < mainApplicantjson.size(); mainapp++) {
						String prefix = "MA";
						if (mainapp > 0) {
							prefix = "MA" + mainapp;
						}
						for (int p = 0; p < cols.size(); p++) {
							System.out
									.println(mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(mainApplicantjson.get(mainapp)
										.get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						mainApplicantjson.get(mainapp).remove("Application Number");
						/////////////////////////////// Main Application Residential Application
						/////////////////////////////// ////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\",COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						@SuppressWarnings("unchecked")
						List<Tuple> mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantRAAll.addAll(mainApplicantRA);

						}
						List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
						prefix = "MA_RA";
						for (int p = 0; p < cols.size(); p++) {
							System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						mainApplicantRAJsom.get(0).remove("Application Number");
						mainApplicantRAJsom.get(0).remove("Customer Number");
						mainApplicantjson.get(mainapp).put("MA_CA", mainApplicantRAJsom.get(0));
						///////////////////////////// Main Application Residential Application End
						///////////////////////////// ///////////////////////////////////////////////////////////////

						///////////////////////////// Main Application Permanant Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\", COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
						}

						mainApplicantRAJsom = _toJson(mainApplicantRA);
						prefix = "MA_PA";
						for (int p = 0; p < cols.size(); p++) {
							System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						mainApplicantRAJsom.get(0).remove("Application Number");
						mainApplicantRAJsom.get(0).remove("Customer Number");
						mainApplicantjson.get(mainapp).put("MA_PMA", mainApplicantRAJsom.get(0));

						/////////////////////////// Main Application Permanant Application End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						///////////////////////////// Main Application Property Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", '') \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							prefix = "MA_PRE";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_PROP", mainApplicantRAJsom.get(0));
						}

						/////////////////////////// Main Application Property Application End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						//////////// MAIN Applicant HOME
						/////////////////////////// Telephone///////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select \"Std Code\"||'-'||\"Phone1\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Std Code\" is not null and \"Customer Number\" in ('"
										+ custNo + "') and \"Std Code\" is not null and \"Phone1\" is not null",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_HT", mainApplicantRAJsom.get(0));
							prefix = "MA_HT";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						///////////////////////// HOME Telephone
						///////////////////////// ////////////////////////////////////////

						/////////////////////////// Main Application Mobile
						/////////////////////////// ///////////////////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select app.\"Application Number\",adds.\"Customer Number\",adds.\"Mobile No\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Mobile No\" is not null and app.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_MT", mainApplicantRAJsom.get(0));
							prefix = "MA_M";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						/////////////////////////// Main Application Mobile
						/////////////////////////// END///////////////////////////////////////////////////////////////

						///////////////////////////// Email
						///////////////////////////// /////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select \"Email ID\" EMA_ADD from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Email ID\" is not null and \"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_EMA", mainApplicantRAJsom.get(0));
							prefix = "MA";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						///////////////////////////////// Email End
						///////////////////////////////// ///////////////////////////////////////////

						/////////////////////////// Main Application Bank
						/////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Bank Name\" BNK_NM,adds.\"Bank Account No\" ACC_NO from NEO_CAS_LMS_SIT1_SH.\"Bank Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Bank Account No\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantBankAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");

							mainApplicantjson.get(mainapp).put("MA_BNK", mainApplicantRAJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p)));
								if (mainApplicantRAJsom.get(0).get(cols.get(p)) != null) {
									row.createCell((short) p)
											.setCellValue(mainApplicantRAJsom.get(0).get(cols.get(p)).asText());
								}
							}
						}
						/////////////////////////// Main Application Bank End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////
						System.out.println("neoCIFID-------------------------------------------------" + neoCIFID);
						q = entityManager.createNativeQuery(
								"select distinct case when \"IDENTIFICATION_TYPE\" like 'Voter_ID' then  'VOTERS CARD' when \"IDENTIFICATION_TYPE\" like 'GOVT_EMP_ID' then  'GOVERNMENT ID CARD' when \"IDENTIFICATION_TYPE\" like 'Driving_Licence' then  'DRIVING LISENCE' when \"IDENTIFICATION_TYPE\" like 'AAdhar_No' then  'AADHAR ID' when \"IDENTIFICATION_TYPE\" like 'Ration_card' then 'RATION CARD' when \"IDENTIFICATION_TYPE\" like 'PASSPORT_No' then 'PASSPORT' when \"IDENTIFICATION_TYPE\" like 'PAN' then 'PAN CARD' else 'OTHER' end DOC_TYP,\"IDENTIFICATION_NUMBER\" DOC_NO from NEO_CAS_LMS_SIT1_SH.\"Identification Details\" where \"IDENTIFICATION_NUMBER\"  is not null and \"IDENTIFICATION_TYPE\" not in ('AAdhar_No') and \"CUSTOMER_INFO_FILE_NUMBER\" in ('"
										+ neoCIFID + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							for (int docid = 0; docid < mainApplicantRAJsom.size(); docid++) {
								prefix = "MA";
								if (docid > 0) {
									prefix = "MA" + docid;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											mainApplicantRAJsom.get(docid).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(docid)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
							}
							ArrayNode array = mapper.valueToTree(mainApplicantRAJsom);
							JsonNode result = mapper.createObjectNode().set("MA_ID", array);
							mainApplicantjson.get(mainapp).set("MADOC", result);
						}

						///////////////////// DOCUMENT ID END

						/////////////////////////// Main Application Employer
						/////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Organization Name\" ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
										+ "when UPPER(adds.\"Industry\") like 'CONSTRUCTION' then 'CONSTRUCTION'\r\n"
										+ "when UPPER(adds.\"Industry\") like 'EDUCATION' then 'EDUCATION'\r\n"
										+ "when adds.\"Industry\" like 'Entertainment' then 'ENTERTAINMENT'\r\n"
										+ "when adds.\"Industry\" like 'Financial Services' then 'FINANCIAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Food and Food Processing' then 'FOOD'\r\n"
										+ "when replace(adds.\"Industry\",'&','') like 'GEMS  JEWELLERY' then 'GEMS AND JEWELLERY'\r\n"
										+ "when adds.\"Industry\" like 'Health Care' then 'HEALTHCARE'\r\n"
										+ "when adds.\"Industry\" like 'Hospitality' then 'HOSPITALITY AND TOURISM'\r\n"
										+ "when adds.\"Industry\" like 'Legal services' then 'LEGAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Manufacturing Unit' then 'MANAFACTURING'\r\n"
										+ "when adds.\"Industry\" like 'Media' then 'MEDIA AND ADVERTISING'\r\n"
										+ "when adds.\"Industry\" like 'Others' then 'OTHER'\r\n"
										+ "when adds.\"Industry\" like 'REAL ESTATE' then 'REAL ESTATE'\r\n"
										+ "when adds.\"Industry\" like 'Retail Shop' then 'RETAIL'\r\n"
										+ "when adds.\"Industry\" like 'Sales and E-Commerce Marketing' then 'SALES'\r\n"
										+ "when adds.\"Industry\" like 'Service Industry' then 'SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Software' then 'INFORMATION TECHNOLOGY'\r\n"
										+ "when adds.\"Industry\" like 'Sports' then 'SPORTS AND LEISURE'\r\n"
										+ "when adds.\"Industry\" like 'TEXTILES' then 'TEXTILES'\r\n"
										+ "when adds.\"Industry\" like 'Truck Driver and Transport' then 'TRANSPORT AND LOGISTICS'\r\n"
										+ "when adds.\"Industry\" like 'Tours And Travel' then 'TRAVEL' else 'OTHER'\r\n"
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and adds.\"Organization Name\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantEmployerAll.addAll(mainApplicantRA);
						}
						mainApplicantRAJsom = _toJson(mainApplicantRA);
						if (!mainApplicantRAJsom.isEmpty()) {
							mainApplicantRAJsom.get(0).remove("Application Number");

							mainApplicantjson.get(mainapp).put("MA_EMP", mainApplicantRAJsom.get(0));

							for (int emp = 0; emp < mainApplicantRAJsom.size(); emp++) {
								prefix = "MA";
								if (emp > 0) {
									prefix = "MA" + emp;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											mainApplicantRAJsom.get(emp).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(emp)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(emp)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}

//Employer Address
								q = entityManager.createNativeQuery(
										"select COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
								prefix = prefix + "_EMP";
								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}

								// Employer Telephone
								q = entityManager.createNativeQuery(
										"select * from (select \"Mobile Number\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where \"Mobile Number\" is not null and adds.\"Addresstype\"='Office/ Business Address' and \"Customer Number\" in ('"
												+ custNo + "')) ds where ds.TEL_NO is not null",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employerTelephone = q.getResultList();

								List<ObjectNode> employerTelephoneJsom = _toJson(employerTelephone);
								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											employerTelephoneJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (employerTelephoneJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
								mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
							}

						}

						/////////////////////////// Main Application Employer
						/////////////////////////// End///////////////////////////////////////////////////////////////
					}
					json.get(app).put("MA", mainApplicantjson.get(0));
				}

				// Main Application Data Population End

				/////////////////////////////// Join
				/////////////////////////////// Applicant///////////////////////////////////////////////////////

				q = entityManager.createNativeQuery(
						"select \"Neo CIF ID\", \"Application Number\",\"Customer Number\",case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Co-Applicant'\r\n"
								+ " and \"Application Number\" in ('" + appNo + "') and rownum<4",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> jointApplicant = q.getResultList();
				String prefix;
				if (jointApplicant != null && !jointApplicant.isEmpty()) {
					jointApplicantAll.addAll(jointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(jointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "JA";
						if (ja > 0) {
							prefix = "JA" + ja;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out.println(jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
							if (jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
						custNo = jointApplicantjson.get(ja).get("Customer Number").asText();
						if (jointApplicantjson.get(ja).has("Neo CIF ID")) {
							neoCIFID = jointApplicantjson.get(ja).get("Neo CIF ID").asText();
						}
						jointApplicantjson.get(ja).remove("Neo CIF ID");
						q = entityManager.createNativeQuery(
								"select  adds.\"Customer Number\",COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						jointApplicantRAAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("Customer Number");

							prefix = prefix + "_RA";

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(jointApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							jointApplicantjson.get(ja).put("JA_CA", jointApplicantRAJsom.get(0));
						}

						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////
						System.out.println("neoCIFID-------------------------------------------------" + neoCIFID);
						q = entityManager.createNativeQuery(
								"select distinct case when \"IDENTIFICATION_TYPE\" like 'Voter_ID' then  'VOTERS CARD' when \"IDENTIFICATION_TYPE\" like 'GOVT_EMP_ID' then  'GOVERNMENT ID CARD' when \"IDENTIFICATION_TYPE\" like 'Driving_Licence' then  'DRIVING LISENCE' when \"IDENTIFICATION_TYPE\" like 'AAdhar_No' then  'AADHAR ID' when \"IDENTIFICATION_TYPE\" like 'Ration_card' then 'RATION CARD' when \"IDENTIFICATION_TYPE\" like 'PASSPORT_No' then 'PASSPORT' when \"IDENTIFICATION_TYPE\" like 'PAN' then 'PAN CARD' else 'OTHER' end DOC_TYP,\"IDENTIFICATION_NUMBER\" DOC_NO from NEO_CAS_LMS_SIT1_SH.\"Identification Details\" where \"IDENTIFICATION_NUMBER\"  is not null and \"IDENTIFICATION_TYPE\" not in ('AAdhar_No') and \"CUSTOMER_INFO_FILE_NUMBER\" in ('"
										+ neoCIFID + "')",
								Tuple.class);
						List<Tuple> jointApplicantdoc = q.getResultList();

						if (jointApplicantdoc != null && jointApplicantdoc.size() > 0) {

							List<ObjectNode> jointApplicantDocJsom = _toJson(jointApplicantdoc);
							for (int docid = 0; docid < jointApplicantDocJsom.size(); docid++) {
								prefix = "JA_RA";
								if (docid > 0) {
									prefix = "JA_RA" + docid;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(jointApplicantDocJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")));
									if (jointApplicantDocJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(jointApplicantDocJsom.get(docid)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
							}
							ArrayNode array = mapper.valueToTree(jointApplicantDocJsom);
							JsonNode result = mapper.createObjectNode().set("JA_ID", array);
							jointApplicantjson.get(ja).set("JADOC", result);
						}

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("JA", array);
					json.get(app).set("JAS", result);

				}

				q = entityManager.createNativeQuery(
						"select app.\"Application Number\" ,par.\"Code\" ORG_CD,par.\"Name\" ORG_NME from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"Code\") where  par.\"Code\" is not null and app.\"Application Number\" in ('"
								+ appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> broker = q.getResultList();

				if (broker != null && !broker.isEmpty()) {
					brokerAll.addAll(broker);
					List<ObjectNode> brokerjson = _toJson(broker);
					for (int br = 0; br < brokerjson.size(); br++) {
						prefix = "BR";
						if (br > 0) {
							prefix = "BR" + br;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out.println(brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")));
							if (brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						brokerjson.get(br).remove("Application Number");
						String refcode = brokerjson.get(br).get("ORG_CD").asText();
						System.out.println(refcode);
						q = entityManager.createNativeQuery(
								"select app.\"Referral Code\",par.\"Address\" \"ADD\",br.\"Branch City\" CTY,br.\"Branch State\" STE,br.\"Branch Pincode\" PIN,br.\"Branch Country\" CTRY from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"Code\") left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (par.\"DSA Branch\"=br.\"Branch Name\") where  trim(par.\"Address\") IS NOT NULL and app.\"Application Number\" in ('"
										+ appNo + "') and app.\"Referral Code\" in ('" + refcode + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> brokerAddress = q.getResultList();
						brokerAddressAll.addAll(brokerAddress);
						if (brokerAddress != null && brokerAddress.size()>0) {
							List<ObjectNode> brokerAddressJsom = _toJson(brokerAddress);
							brokerAddressJsom.get(0).remove("Referral Code");
							brokerjson.get(br).put("BR_ADD", brokerAddressJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(brokerAddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (brokerAddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(brokerAddressJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
						}

					}

					json.get(app).put("BR", brokerjson.get(0));
				}

				/////////////////////////////// Join Applicant
				/////////////////////////////// END///////////////////////////////////////////////////////

				////////////////////////////// REFERENCES DETAILS
				////////////////////////////// ////////////////////////////////

				q = entityManager.createNativeQuery(
						"select  APPLICATION_NUMBER,regexp_substr(NAME ,'[^ - ]+',1,1) FST_NME,case when regexp_substr(NAME ,'[^ - ]+',1,3) is null then regexp_substr(NAME ,'[^ - ]+',1,2) else regexp_substr(NAME ,'[^ - ]+',1,3) end LST_NME from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS where APPLICATION_NUMBER in ('"
								+ appNo + "') and NAME is not null",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> referenceApplicant = q.getResultList();

				if (referenceApplicant != null && !referenceApplicant.isEmpty()) {
					referenceApplicantAll.addAll(referenceApplicant);
					List<ObjectNode> referenceApplicantjson = _toJson(referenceApplicant);

					for (int ja = 0; ja < referenceApplicantjson.size(); ja++) {

						prefix = "RF";
						if (ja > 0) {
							prefix = "RF" + ja;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out
									.println(referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
							if (referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
										.get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						referenceApplicantjson.get(ja).remove("APPLICATION_NUMBER");
						System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
						q = entityManager.createNativeQuery(
								"select APPLICATION_NUMBER,COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\", \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in  ('"
										+ appNo + "') and adds.\"Address 1\" is not null ",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> referenceApplicantRA = q.getResultList();
						referenceApplicantRAAll.addAll(referenceApplicantRA);

						List<ObjectNode> referenceApplicantRAJsom = _toJson(referenceApplicantRA);
						if (!referenceApplicantRAJsom.isEmpty()) {
							referenceApplicantRAJsom.get(0).remove("APPLICATION_NUMBER");
							referenceApplicantjson.get(ja).put("RF_CA", referenceApplicantRAJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
								if (referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
						}

						q = entityManager.createNativeQuery(
								"select APPLICATION_NUMBER,TO_CHAR(\"Mobile Number\") TEL_NO from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in ('"
										+ appNo + "') and \"Mobile Number\" is not null",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						referencetApplicantMobileAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("APPLICATION_NUMBER");
							referenceApplicantjson.get(ja).put("RF_MT", jointApplicantRAJsom.get(0));
						}

					}
					ArrayNode array = mapper.valueToTree(referenceApplicantjson);
					JsonNode result = mapper.createObjectNode().set("RF", array);
					json.get(app).set("RFS", result);

				}

				//////////////////////////////////////// REFERENCE
				//////////////////////////////////////// END//////////////////////////////

			}
			//////////////////////////////// GENERATE XML FILE and SEND
			//////////////////////////////// Email///////////////////
			ObjectNode root = mapper.createObjectNode();
			ObjectNode batch = mapper.createObjectNode();
			ObjectNode header = mapper.createObjectNode();
			header.put("COUNT", fetchapplication.size());
			header.put("ORIGINATOR", "SHDFC");
			batch.put("HEADER", header);

			for (int js = 0; js < json.size(); js++) {
				json.get(js).remove("Application Number");
				json.get(js).remove("Customer Number");
				json.get(js).remove("Neo CIF ID");

			}
			ArrayNode array = mapper.valueToTree(json);
			JsonNode result = mapper.createObjectNode().set("SUBMISSION", array);
			batch.put("SUBMISSIONS", result);
			root.set("BATCH", batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			// System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`() from dual";

			String fileNo = osourceTemplate.queryForObject(sql, String.class);

			//////////////////////////// GENERATE XML FILE and Send Email

			////////// GENERATE XLS LOGIC/////////////////////////////////////////////

			if (fetchapplication != null && fetchapplication.size() > 0) {

				FileOutputStream fileOut = new FileOutputStream(fileNo + ".xlsx");
				workbook.write(fileOut);
				fileOut.close();

				System.out.println("file generated successfully");

				/// GENERATE XML AND SEND EMAIL//////////////////////////////
				boolean filepath = stringToDomAdhoc(createXml, fileNo + ".xml", hour);

				/*
				 * GeneratedKeyHolder holder = new GeneratedKeyHolder();
				 * osourceTemplate.update(new PreparedStatementCreator() {
				 * 
				 * @Override public PreparedStatement createPreparedStatement(Connection con)
				 * throws SQLException { PreparedStatement statement = con.prepareStatement(
				 * "INSERT INTO hunter_job (emailsend, createon, jobfile, jobdataxml, emailsendon) VALUES (?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP) "
				 * , Statement.RETURN_GENERATED_KEYS); statement.setString(1,
				 * String.valueOf(filepath ? 1 : 0)); statement.setString(2, fileNo);
				 * statement.setString(3, createXml); return statement; } }, holder);
				 * 
				 * long primaryKey = holder.getKey().longValue();
				 * 
				 * String sqls =
				 * "INSERT INTO `hunter_job_application` (`createon`, `applicationnumber`, `hunter_job_id`) VALUES (CURRENT_TIMESTAMP, ?, ?)"
				 * ;
				 * 
				 * List<Object[]> parameters = new ArrayList<Object[]>();
				 * 
				 * for (String cust : appList) { parameters.add(new Object[] { cust, primaryKey
				 * }); } osourceTemplate.batchUpdate(sqls, parameters);
				 */
				System.out.println(createXml);
				JsonNode resultjson = mapper.createObjectNode().set("SUBMISSION", array);
				System.out.println(resultjson.asText());

			}

			//////////////////////////// End/////////////////////
			/////////////////////////////////////////

		} catch (Exception e) {
			e.printStackTrace();
		}

		return json;

	}
	
	///////////////////////////////////////////////////////////////////////////////////DAILY HUNTER DATA START
	@GetMapping("/fetchData")
	public List<ObjectNode> fetchData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME", "MA_LST_NME", "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD",
				"MA_PA_CTY", "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY", "MA_RA_STE",
				"MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY", "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN",
				"MA_HT_TEL_NO", "MA_M_TEL_NO", "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
				"MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP", "MA2_DOC_NO", "MA3_DOC_TYP",
				"MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO", "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO",
				"MA7_DOC_TYP", "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO", "MA_ORG_NME",
				"MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD", "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY",
				"MA_EMP_PIN", "MA_EMP_TEL", "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
				"MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB", "JA_AGE", "JA_GNDR",
				"JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME", "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1",
				"JA2_PAN", "JA2_FST_NME", "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
				"JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD", "JA1_RA_CTY", "JA1_RA_STE",
				"JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD", "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN",
				"JA_RA_DOC_TYP_1", "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
				"JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5", "JA_RA_DOC_NO_5",
				"JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7", "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8",
				"JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9", "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10",
				"JA1_RA_DOC_TYP_1", "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
				"JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5", "JA1_RA_DOC_NO_5",
				"JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7", "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8",
				"JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9", "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
				"JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2", "JA2_RA_DOC_TYP_3",
				"JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4", "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5",
				"JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6", "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8",
				"JA2_RA_DOC_NO_8", "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10", "JA2_RA_DOC_NO_10",
				"RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME", "RF2_FST_NME", "RF2_LST_NME", "RF_ADD",
				"RF_CTY", "RF_STE", "RF_CTRY", "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN",
				"RF2_ADD", "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO", "RF2_TEL_NO",
				"RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME", "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE",
				"BR_CTRY", "BR_PIN" };
		ObjectMapper mapper = new ObjectMapper();
		List<ObjectNode> json = new ArrayList<>();
		SimpleDateFormat querydate = new SimpleDateFormat("DD-MMM-YY");
		SimpleDateFormat formatDate = new SimpleDateFormat("hh:mm a");
		String hour = formatDate.format(new Date());
		System.out.println(hour);
		String querysubmission = null;
		Set<String> appList = new HashSet<>();
		List<Tuple> fetchapplication = new ArrayList<>();
		List<Tuple> mainApplicantAll = new ArrayList<>();
		List<Tuple> mainApplicantRAAll = new ArrayList<>();
		List<Tuple> mainApplicantPAAll = new ArrayList<>();
		List<Tuple> mainApplicantMobileAll = new ArrayList<>();
		List<Tuple> mainApplicantBankAll = new ArrayList<>();
		List<Tuple> mainApplicantEmployerAll = new ArrayList<>();
		List<Tuple> jointApplicantAll = new ArrayList<>();
		List<Tuple> jointApplicantRAAll = new ArrayList<>();
		List<Tuple> brokerAll = new ArrayList<>();
		List<Tuple> brokerAddressAll = new ArrayList<>();
		List<Tuple> referenceApplicantAll = new ArrayList<>();
		List<Tuple> referenceApplicantRAAll = new ArrayList<>();
		List<Tuple> referencetApplicantMobileAll = new ArrayList<>();
		XSSFWorkbook workbook = new XSSFWorkbook();
		XSSFSheet huntersheet = workbook.createSheet("HUNTER");
		Row headerRow = huntersheet.createRow(0);
		List<String> cols = Arrays.asList(headerArray);
		System.out.println(cols.size());
		for (int i = 0; i < cols.size(); i++) {
			String headerVal = cols.get(i).toString();
			Cell headerCell = headerRow.createCell(i);
			headerCell.setCellValue(headerVal);

		}

		try {
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(CURDATE(),'%d-%b-%y') todate,DATE_FORMAT(date(CURDATE()-1),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;
			if (hour.contains("AM")) {
				querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,case when app.\"Product Type Code\" like 'HL' then 'HL_IND' else 'NHL_IND' end PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Preet Vihar' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='BHOPAL' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='SANGLI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='MEERUT' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='WARDHA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Naroda' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDNAGAR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JHANSI' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='AKOLA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='GWALIOR' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='MATHURA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='DHULE' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='SAGAR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='JAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='RAIPUR' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='NAGPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='BOKARO' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRAVATI' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='PATNA' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='Rewari' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='Vasai' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILAI' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='MADANGIR' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='SHRIRAMPUR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='BULDHANA' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SATARA' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KALYAN' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='FARIDABAD' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='INDORE' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILWARA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='Sonipat' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='PANIPAT' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='MANGOLPURI' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMSHEDPUR' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='JABALPUR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='MUZAFFARPUR' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='GURGAON' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEHRADUN' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='Patiala' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='UDAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='YAVATMAL' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='VARANASI' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PUNE' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KANPUR' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOTA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ALLAHABAD' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PARBHANI' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='RAJKOT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='PIMPRI CHINWAD' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='LUCKNOW' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='LATUR' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SURAT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALANDHAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='AJMER' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='LUDHIANA' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='CHANDRAPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Bhatinda' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='KARNAL' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='BELAPUR' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='SAHARANPUR' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='BAREILLY' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOLHAPUR' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='AURANGABAD' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAGATPURA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ROORKEE' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='VADODARA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='RANCHI' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMBALA' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='AGRA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='NASHIK' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMNAGAR' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JODHPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='BARAMATI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='Chandan Nagar' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='NANDED' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='JUNAGADH' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Janakpuri' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRITSAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='Noida' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEWAS' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='WAPI' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALGAON' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='UJJAIN' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDABAD' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Rohtak' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='Panvel' THEN 'Mumbai'\r\n" + "ELSE  br.\"Branch Name\"\r\n"
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.VERDICT_DATE between TO_TIMESTAMP ('"
						+ mailconfig.getFromdate() + " 16:00:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('"
						+ mailconfig.getTodate() + " 08:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\") ";

			} else if (hour.contains("PM")) {
				querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,case when app.\"Product Type Code\" like 'HL' then 'HL_IND' else 'NHL_IND' end PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Preet Vihar' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='MEHSANA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='BHOPAL' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='SANGLI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='MEERUT' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='WARDHA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Naroda' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDNAGAR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JHANSI' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='AKOLA' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='GWALIOR' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='MATHURA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='DHULE' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='SAGAR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='JAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='RAIPUR' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='NAGPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='BOKARO' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRAVATI' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='PATNA' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='Rewari' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='Vasai' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILAI' THEN 'Chhatisgarh'\r\n"
						+ "WHEN br.\"Branch Name\"='MADANGIR' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='SHRIRAMPUR' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='BULDHANA' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SATARA' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KALYAN' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='FARIDABAD' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='INDORE' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='BHILWARA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='Sonipat' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='PANIPAT' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='MANGOLPURI' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMSHEDPUR' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='JABALPUR' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='MUZAFFARPUR' THEN 'Bihar'\r\n"
						+ "WHEN br.\"Branch Name\"='GURGAON' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEHRADUN' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='Patiala' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='UDAIPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='YAVATMAL' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='VARANASI' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PUNE' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='KANPUR' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOTA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ALLAHABAD' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='PARBHANI' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='RAJKOT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='PIMPRI CHINWAD' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='LUCKNOW' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='LATUR' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='SURAT' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALANDHAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='AJMER' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='LUDHIANA' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='CHANDRAPUR' THEN 'MH - Vidarbha'\r\n"
						+ "WHEN br.\"Branch Name\"='Bhatinda' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='KARNAL' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='BELAPUR' THEN 'Mumbai'\r\n"
						+ "WHEN br.\"Branch Name\"='SAHARANPUR' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='BAREILLY' THEN 'UP - Lucknow'\r\n"
						+ "WHEN br.\"Branch Name\"='KOLHAPUR' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='AURANGABAD' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAGATPURA' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='ROORKEE' THEN 'UP - Meerut'\r\n"
						+ "WHEN br.\"Branch Name\"='VADODARA' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='RANCHI' THEN 'Jharkhand'\r\n"
						+ "WHEN br.\"Branch Name\"='AMBALA' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='AGRA' THEN 'UP - Agra'\r\n"
						+ "WHEN br.\"Branch Name\"='NASHIK' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='JAMNAGAR' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JODHPUR' THEN 'Rajasthan'\r\n"
						+ "WHEN br.\"Branch Name\"='BARAMATI' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='Chandan Nagar' THEN 'MH - Pune'\r\n"
						+ "WHEN br.\"Branch Name\"='NANDED' THEN 'MH - Latur'\r\n"
						+ "WHEN br.\"Branch Name\"='JUNAGADH' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Janakpuri' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='AMRITSAR' THEN 'Punjab'\r\n"
						+ "WHEN br.\"Branch Name\"='Noida' THEN 'Delhi'\r\n"
						+ "WHEN br.\"Branch Name\"='DEWAS' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='WAPI' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='JALGAON' THEN 'MH - Marathwada'\r\n"
						+ "WHEN br.\"Branch Name\"='UJJAIN' THEN 'MP'\r\n"
						+ "WHEN br.\"Branch Name\"='AHMEDABAD' THEN 'Gujarat'\r\n"
						+ "WHEN br.\"Branch Name\"='Rohtak' THEN 'Haryana'\r\n"
						+ "WHEN br.\"Branch Name\"='Panvel' THEN 'Mumbai'\r\n" + "ELSE  br.\"Branch Name\"\r\n"
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.VERDICT_DATE between TO_TIMESTAMP ('"
						+ mailconfig.getTodate() + " 09:00:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('"
						+ mailconfig.getTodate() + " 15:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\")";
			}

			String dbapp = "SELECT applicationnumber from hunter_job_application";

			List<String> dbapplist = osourceTemplate.queryForList(dbapp, String.class);

			if (dbapplist != null && dbapplist.size() > 0) {

				List<List<String>> partitions = Lists.partition(dbapplist, 999);
				System.out.println(partitions.size());

				for (int p = 0; p < partitions.size(); p++) {
					querysubmission += " and app.\"Application Number\" not in ("
							+ partitions.get(p).stream().collect(Collectors.joining("','", "'", "'")) + ")";

				}
				q = entityManager.createNativeQuery(querysubmission, Tuple.class);

			} else {
				q = entityManager.createNativeQuery(querysubmission, Tuple.class);
			}

			fetchapplication = q.getResultList();

			json = _toJson(fetchapplication);
			int rowCount = 1;
			for (int app = 0; app < json.size(); app++) {

				Row row = huntersheet.createRow(rowCount++);
				for (int p = 0; p < cols.size(); p++) {
					if (json.get(app).get(cols.get(p)) != null) {
						row.createCell((short) p).setCellValue(json.get(app).get(cols.get(p)).asText());
					}
				}
				String appNo = json.get(app).get("Application Number").asText();
				appList.add(appNo);
				String custNo = json.get(app).get("Customer Number").asText();
				System.out.println(appNo + "                    " + custNo);

				String neoCIFID = null;
				if (json.get(app).has("Neo CIF ID")) {
					neoCIFID = json.get(app).get("Neo CIF ID").asText();
				}
				System.out.println(neoCIFID + "                    " + neoCIFID);

				// Main Application Data Population End
				q = entityManager.createNativeQuery(
						"select \"Application Number\", case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'\r\n"
								+ " and \"Identification Type\" is not null and \"Application Number\" in ('" + appNo
								+ "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				if (mainApplicant != null && !mainApplicant.isEmpty()) {
					mainApplicantAll.addAll(mainApplicant);

					List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
					for (int mainapp = 0; mainapp < mainApplicantjson.size(); mainapp++) {
						String prefix = "MA";
						if (mainapp > 0) {
							prefix = "MA" + mainapp;
						}
						for (int p = 0; p < cols.size(); p++) {
							System.out
									.println(mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(mainApplicantjson.get(mainapp)
										.get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						mainApplicantjson.get(mainapp).remove("Application Number");
						/////////////////////////////// Main Application Residential Application
						/////////////////////////////// ////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\",COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						@SuppressWarnings("unchecked")
						List<Tuple> mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantRAAll.addAll(mainApplicantRA);

						}
						List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
						prefix = "MA_RA";
						for (int p = 0; p < cols.size(); p++) {
							System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						mainApplicantRAJsom.get(0).remove("Application Number");
						mainApplicantRAJsom.get(0).remove("Customer Number");
						mainApplicantjson.get(mainapp).put("MA_CA", mainApplicantRAJsom.get(0));
						///////////////////////////// Main Application Residential Application End
						///////////////////////////// ///////////////////////////////////////////////////////////////

						///////////////////////////// Main Application Permanant Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\", COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
						}

						mainApplicantRAJsom = _toJson(mainApplicantRA);
						prefix = "MA_PA";
						for (int p = 0; p < cols.size(); p++) {
							System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
							if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						mainApplicantRAJsom.get(0).remove("Application Number");
						mainApplicantRAJsom.get(0).remove("Customer Number");
						mainApplicantjson.get(mainapp).put("MA_PMA", mainApplicantRAJsom.get(0));

						/////////////////////////// Main Application Permanant Application End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						///////////////////////////// Main Application Property Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", '') \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							prefix = "MA_PRE";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_PROP", mainApplicantRAJsom.get(0));
						}

						/////////////////////////// Main Application Property Application End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						//////////// MAIN Applicant HOME
						/////////////////////////// Telephone///////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select \"Std Code\"||'-'||\"Phone1\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Std Code\" is not null and \"Customer Number\" in ('"
										+ custNo + "') and \"Std Code\" is not null and \"Phone1\" is not null",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_HT", mainApplicantRAJsom.get(0));
							prefix = "MA_HT";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						///////////////////////// HOME Telephone
						///////////////////////// ////////////////////////////////////////

						/////////////////////////// Main Application Mobile
						/////////////////////////// ///////////////////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select app.\"Application Number\",adds.\"Customer Number\",adds.\"Mobile No\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Mobile No\" is not null and app.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_MT", mainApplicantRAJsom.get(0));
							prefix = "MA_M";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						/////////////////////////// Main Application Mobile
						/////////////////////////// END///////////////////////////////////////////////////////////////

						///////////////////////////// Email
						///////////////////////////// /////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select \"Email ID\" EMA_ADD from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Email ID\" is not null and \"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_EMA", mainApplicantRAJsom.get(0));
							prefix = "MA";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}

						///////////////////////////////// Email End
						///////////////////////////////// ///////////////////////////////////////////

						/////////////////////////// Main Application Bank
						/////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Bank Name\" BNK_NM,adds.\"Bank Account No\" ACC_NO from NEO_CAS_LMS_SIT1_SH.\"Bank Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Bank Account No\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantBankAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");

							mainApplicantjson.get(mainapp).put("MA_BNK", mainApplicantRAJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p)));
								if (mainApplicantRAJsom.get(0).get(cols.get(p)) != null) {
									row.createCell((short) p)
											.setCellValue(mainApplicantRAJsom.get(0).get(cols.get(p)).asText());
								}
							}
						}
						/////////////////////////// Main Application Bank End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////
						System.out.println("neoCIFID-------------------------------------------------" + neoCIFID);
						q = entityManager.createNativeQuery(
								"select distinct case when \"IDENTIFICATION_TYPE\" like 'Voter_ID' then  'VOTERS CARD' when \"IDENTIFICATION_TYPE\" like 'GOVT_EMP_ID' then  'GOVERNMENT ID CARD' when \"IDENTIFICATION_TYPE\" like 'Driving_Licence' then  'DRIVING LISENCE' when \"IDENTIFICATION_TYPE\" like 'AAdhar_No' then  'AADHAR ID' when \"IDENTIFICATION_TYPE\" like 'Ration_card' then 'RATION CARD' when \"IDENTIFICATION_TYPE\" like 'PASSPORT_No' then 'PASSPORT' when \"IDENTIFICATION_TYPE\" like 'PAN' then 'PAN CARD' else 'OTHER' end DOC_TYP,\"IDENTIFICATION_NUMBER\" DOC_NO from NEO_CAS_LMS_SIT1_SH.\"Identification Details\" where \"IDENTIFICATION_NUMBER\"  is not null and \"IDENTIFICATION_TYPE\" not in ('AAdhar_No') and \"CUSTOMER_INFO_FILE_NUMBER\" in ('"
										+ neoCIFID + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							mainApplicantRAJsom = _toJson(mainApplicantRA);
							for (int docid = 0; docid < mainApplicantRAJsom.size(); docid++) {
								prefix = "MA";
								if (docid > 0) {
									prefix = "MA" + docid;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											mainApplicantRAJsom.get(docid).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(docid)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
							}
							ArrayNode array = mapper.valueToTree(mainApplicantRAJsom);
							JsonNode result = mapper.createObjectNode().set("MA_ID", array);
							mainApplicantjson.get(mainapp).set("MADOC", result);
						}

						///////////////////// DOCUMENT ID END

						/////////////////////////// Main Application Employer
						/////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Organization Name\" ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
										+ "when UPPER(adds.\"Industry\") like 'CONSTRUCTION' then 'CONSTRUCTION'\r\n"
										+ "when UPPER(adds.\"Industry\") like 'EDUCATION' then 'EDUCATION'\r\n"
										+ "when adds.\"Industry\" like 'Entertainment' then 'ENTERTAINMENT'\r\n"
										+ "when adds.\"Industry\" like 'Financial Services' then 'FINANCIAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Food and Food Processing' then 'FOOD'\r\n"
										+ "when replace(adds.\"Industry\",'&','') like 'GEMS  JEWELLERY' then 'GEMS AND JEWELLERY'\r\n"
										+ "when adds.\"Industry\" like 'Health Care' then 'HEALTHCARE'\r\n"
										+ "when adds.\"Industry\" like 'Hospitality' then 'HOSPITALITY AND TOURISM'\r\n"
										+ "when adds.\"Industry\" like 'Legal services' then 'LEGAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Manufacturing Unit' then 'MANAFACTURING'\r\n"
										+ "when adds.\"Industry\" like 'Media' then 'MEDIA AND ADVERTISING'\r\n"
										+ "when adds.\"Industry\" like 'Others' then 'OTHER'\r\n"
										+ "when adds.\"Industry\" like 'REAL ESTATE' then 'REAL ESTATE'\r\n"
										+ "when adds.\"Industry\" like 'Retail Shop' then 'RETAIL'\r\n"
										+ "when adds.\"Industry\" like 'Sales and E-Commerce Marketing' then 'SALES'\r\n"
										+ "when adds.\"Industry\" like 'Service Industry' then 'SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Software' then 'INFORMATION TECHNOLOGY'\r\n"
										+ "when adds.\"Industry\" like 'Sports' then 'SPORTS AND LEISURE'\r\n"
										+ "when adds.\"Industry\" like 'TEXTILES' then 'TEXTILES'\r\n"
										+ "when adds.\"Industry\" like 'Truck Driver and Transport' then 'TRANSPORT AND LOGISTICS'\r\n"
										+ "when adds.\"Industry\" like 'Tours And Travel' then 'TRAVEL' else 'OTHER'\r\n"
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and adds.\"Organization Name\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantEmployerAll.addAll(mainApplicantRA);
						}
						mainApplicantRAJsom = _toJson(mainApplicantRA);
						if (!mainApplicantRAJsom.isEmpty()) {
							mainApplicantRAJsom.get(0).remove("Application Number");

							mainApplicantjson.get(mainapp).put("MA_EMP", mainApplicantRAJsom.get(0));

							for (int emp = 0; emp < mainApplicantRAJsom.size(); emp++) {
								prefix = "MA";
								if (emp > 0) {
									prefix = "MA" + emp;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											mainApplicantRAJsom.get(emp).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(emp)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(emp)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}

//Employer Address
								q = entityManager.createNativeQuery(
										"select COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
								prefix = prefix + "_EMP";
								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}

								// Employer Telephone
								q = entityManager.createNativeQuery(
										"select * from (select \"Mobile Number\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where \"Mobile Number\" is not null and adds.\"Addresstype\"='Office/ Business Address' and \"Customer Number\" in ('"
												+ custNo + "')) ds where ds.TEL_NO is not null",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employerTelephone = q.getResultList();

								List<ObjectNode> employerTelephoneJsom = _toJson(employerTelephone);
								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											employerTelephoneJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (employerTelephoneJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
								mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
							}

						}

						/////////////////////////// Main Application Employer
						/////////////////////////// End///////////////////////////////////////////////////////////////
					}
					json.get(app).put("MA", mainApplicantjson.get(0));
				}

				// Main Application Data Population End

				/////////////////////////////// Join
				/////////////////////////////// Applicant///////////////////////////////////////////////////////

				q = entityManager.createNativeQuery(
						"select \"Neo CIF ID\", \"Application Number\",\"Customer Number\",case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Co-Applicant'\r\n"
								+ " and \"Application Number\" in ('" + appNo + "') and rownum<4",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> jointApplicant = q.getResultList();
				String prefix;
				if (jointApplicant != null && !jointApplicant.isEmpty()) {
					jointApplicantAll.addAll(jointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(jointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "JA";
						if (ja > 0) {
							prefix = "JA" + ja;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out.println(jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
							if (jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
						custNo = jointApplicantjson.get(ja).get("Customer Number").asText();
						if (jointApplicantjson.get(ja).has("Neo CIF ID")) {
							neoCIFID = jointApplicantjson.get(ja).get("Neo CIF ID").asText();
						}
						jointApplicantjson.get(ja).remove("Neo CIF ID");
						q = entityManager.createNativeQuery(
								"select  adds.\"Customer Number\",COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						jointApplicantRAAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("Customer Number");

							prefix = prefix + "_RA";

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(jointApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							jointApplicantjson.get(ja).put("JA_CA", jointApplicantRAJsom.get(0));
						}

						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////
						System.out.println("neoCIFID-------------------------------------------------" + neoCIFID);
						q = entityManager.createNativeQuery(
								"select distinct case when \"IDENTIFICATION_TYPE\" like 'Voter_ID' then  'VOTERS CARD' when \"IDENTIFICATION_TYPE\" like 'GOVT_EMP_ID' then  'GOVERNMENT ID CARD' when \"IDENTIFICATION_TYPE\" like 'Driving_Licence' then  'DRIVING LISENCE' when \"IDENTIFICATION_TYPE\" like 'AAdhar_No' then  'AADHAR ID' when \"IDENTIFICATION_TYPE\" like 'Ration_card' then 'RATION CARD' when \"IDENTIFICATION_TYPE\" like 'PASSPORT_No' then 'PASSPORT' when \"IDENTIFICATION_TYPE\" like 'PAN' then 'PAN CARD' else 'OTHER' end DOC_TYP,\"IDENTIFICATION_NUMBER\" DOC_NO from NEO_CAS_LMS_SIT1_SH.\"Identification Details\" where \"IDENTIFICATION_NUMBER\"  is not null and \"IDENTIFICATION_TYPE\" not in ('AAdhar_No') and \"CUSTOMER_INFO_FILE_NUMBER\" in ('"
										+ neoCIFID + "')",
								Tuple.class);
						List<Tuple> jointApplicantdoc = q.getResultList();

						if (jointApplicantdoc != null && jointApplicantdoc.size() > 0) {

							List<ObjectNode> jointApplicantDocJsom = _toJson(jointApplicantdoc);
							for (int docid = 0; docid < jointApplicantDocJsom.size(); docid++) {
								prefix = "JA_RA";
								if (docid > 0) {
									prefix = "JA_RA" + docid;
								}

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(jointApplicantDocJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")));
									if (jointApplicantDocJsom.get(docid)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(jointApplicantDocJsom.get(docid)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
							}
							ArrayNode array = mapper.valueToTree(jointApplicantDocJsom);
							JsonNode result = mapper.createObjectNode().set("JA_ID", array);
							jointApplicantjson.get(ja).set("JADOC", result);
						}

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("JA", array);
					json.get(app).set("JAS", result);

				}

				q = entityManager.createNativeQuery(
						"select app.\"Application Number\" ,par.\"Code\" ORG_CD,par.\"Name\" ORG_NME from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"Code\") where  par.\"Code\" is not null and app.\"Application Number\" in ('"
								+ appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> broker = q.getResultList();

				if (broker != null && !broker.isEmpty()) {
					brokerAll.addAll(broker);
					List<ObjectNode> brokerjson = _toJson(broker);
					for (int br = 0; br < brokerjson.size(); br++) {
						prefix = "BR";
						if (br > 0) {
							prefix = "BR" + br;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out.println(brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")));
							if (brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(
										brokerjson.get(br).get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}

						brokerjson.get(br).remove("Application Number");
						String refcode = brokerjson.get(br).get("ORG_CD").asText();
						System.out.println(refcode);
						q = entityManager.createNativeQuery(
								"select app.\"Referral Code\",par.\"Address\" \"ADD\",br.\"Branch City\" CTY,br.\"Branch State\" STE,br.\"Branch Pincode\" PIN,br.\"Branch Country\" CTRY from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"Code\") left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (par.\"DSA Branch\"=br.\"Branch Name\") where  trim(par.\"Address\") IS NOT NULL and app.\"Application Number\" in ('"
										+ appNo + "') and app.\"Referral Code\" in ('" + refcode + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> brokerAddress = q.getResultList();
						brokerAddressAll.addAll(brokerAddress);
						if (brokerAddress != null && brokerAddress.size()>0) {
							List<ObjectNode> brokerAddressJsom = _toJson(brokerAddress);
							brokerAddressJsom.get(0).remove("Referral Code");
							brokerjson.get(br).put("BR_ADD", brokerAddressJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(brokerAddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (brokerAddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(brokerAddressJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
						}

					}

					json.get(app).put("BR", brokerjson.get(0));
				}

				/////////////////////////////// Join Applicant
				/////////////////////////////// END///////////////////////////////////////////////////////

				////////////////////////////// REFERENCES DETAILS
				////////////////////////////// ////////////////////////////////

				q = entityManager.createNativeQuery(
						"select  APPLICATION_NUMBER,regexp_substr(NAME ,'[^ - ]+',1,1) FST_NME,case when regexp_substr(NAME ,'[^ - ]+',1,3) is null then regexp_substr(NAME ,'[^ - ]+',1,2) else regexp_substr(NAME ,'[^ - ]+',1,3) end LST_NME from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS where APPLICATION_NUMBER in ('"
								+ appNo + "') and NAME is not null",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> referenceApplicant = q.getResultList();

				if (referenceApplicant != null && !referenceApplicant.isEmpty()) {
					referenceApplicantAll.addAll(referenceApplicant);
					List<ObjectNode> referenceApplicantjson = _toJson(referenceApplicant);

					for (int ja = 0; ja < referenceApplicantjson.size(); ja++) {

						prefix = "RF";
						if (ja > 0) {
							prefix = "RF" + ja;
						}

						for (int p = 0; p < cols.size(); p++) {
							System.out
									.println(referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
							if (referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
								row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
										.get(cols.get(p).replace(prefix + "_", "")).asText());
							}
						}
						referenceApplicantjson.get(ja).remove("APPLICATION_NUMBER");
						System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
						q = entityManager.createNativeQuery(
								"select APPLICATION_NUMBER,COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\", \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in  ('"
										+ appNo + "') and adds.\"Address 1\" is not null ",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> referenceApplicantRA = q.getResultList();
						referenceApplicantRAAll.addAll(referenceApplicantRA);

						List<ObjectNode> referenceApplicantRAJsom = _toJson(referenceApplicantRA);
						if (!referenceApplicantRAJsom.isEmpty()) {
							referenceApplicantRAJsom.get(0).remove("APPLICATION_NUMBER");
							referenceApplicantjson.get(ja).put("RF_CA", referenceApplicantRAJsom.get(0));

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
								if (referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
						}

						q = entityManager.createNativeQuery(
								"select APPLICATION_NUMBER,TO_CHAR(\"Mobile Number\") TEL_NO from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in ('"
										+ appNo + "') and \"Mobile Number\" is not null",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						referencetApplicantMobileAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("APPLICATION_NUMBER");
							referenceApplicantjson.get(ja).put("RF_MT", jointApplicantRAJsom.get(0));
						}

					}
					ArrayNode array = mapper.valueToTree(referenceApplicantjson);
					JsonNode result = mapper.createObjectNode().set("RF", array);
					json.get(app).set("RFS", result);

				}

				//////////////////////////////////////// REFERENCE
				//////////////////////////////////////// END//////////////////////////////

			}
			//////////////////////////////// GENERATE XML FILE and SEND
			//////////////////////////////// Email///////////////////
			ObjectNode root = mapper.createObjectNode();
			ObjectNode batch = mapper.createObjectNode();
			ObjectNode header = mapper.createObjectNode();
			header.put("COUNT", fetchapplication.size());
			header.put("ORIGINATOR", "SHDFC");
			batch.put("HEADER", header);

			for (int js = 0; js < json.size(); js++) {
				json.get(js).remove("Application Number");
				json.get(js).remove("Customer Number");
				json.get(js).remove("Neo CIF ID");

			}
			ArrayNode array = mapper.valueToTree(json);
			JsonNode result = mapper.createObjectNode().set("SUBMISSION", array);
			batch.put("SUBMISSIONS", result);
			root.set("BATCH", batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			// System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`() from dual";

			String fileNo = osourceTemplate.queryForObject(sql, String.class);

			//////////////////////////// GENERATE XML FILE and Send Email

			////////// GENERATE XLS LOGIC/////////////////////////////////////////////

			if (fetchapplication != null && fetchapplication.size() > 0) {

				FileOutputStream fileOut = new FileOutputStream(fileNo + ".xlsx");
				workbook.write(fileOut);
				fileOut.close();

				System.out.println("file generated successfully");

				/// GENERATE XML AND SEND EMAIL//////////////////////////////
				boolean filepath = stringToDom(createXml, fileNo + ".xml", hour);

				GeneratedKeyHolder holder = new GeneratedKeyHolder();
				osourceTemplate.update(new PreparedStatementCreator() {
					@Override
					public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
						PreparedStatement statement = con.prepareStatement(
								"INSERT INTO hunter_job (emailsend, createon, jobfile, jobdataxml, emailsendon) VALUES (?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP) ",
								Statement.RETURN_GENERATED_KEYS);
						statement.setString(1, String.valueOf(filepath ? 1 : 0));
						statement.setString(2, fileNo);
						statement.setString(3, createXml);
						return statement;
					}
				}, holder);

				long primaryKey = holder.getKey().longValue();

				String sqls = "INSERT INTO `hunter_job_application` (`createon`, `applicationnumber`, `hunter_job_id`) VALUES (CURRENT_TIMESTAMP, ?, ?)";

				List<Object[]> parameters = new ArrayList<Object[]>();

				for (String cust : appList) {
					parameters.add(new Object[] { cust, primaryKey });
				}
				osourceTemplate.batchUpdate(sqls, parameters);
				System.out.println(createXml);
				JsonNode resultjson = mapper.createObjectNode().set("SUBMISSION", array);
				System.out.println(resultjson.asText());

			}

			//////////////////////////// End/////////////////////
			/////////////////////////////////////////

		} catch (Exception e) {
			e.printStackTrace();
		}

		return json;

	}

	@GetMapping("/fetchDataJson/{startdate}/{enddate}/{applicationnumber}")
	public List<ObjectNode> fetchDataJson(@PathVariable("startdate") String startdate,
			@PathVariable("enddate") String enddate, @PathVariable("applicationnumber") String applicationnumber) {

		System.out.println(startdate + "         " + enddate);
		ObjectMapper mapper = new ObjectMapper();

		Set<String> appList = new HashSet<>();
		String querysubmission = "select app.\"Customer Number\" ,app.\"Application Number\",case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'A' end||'_'||app.\"Application Number\"||'_'||'HOU'  IDENTIFIER,case when app.\"Product Type Code\" like 'HL' then 'HL_IND' else 'NHL_IND' end PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,app.\"Branch Code\" BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.\"Sanction Date\" between '"
				+ startdate + "' and '" + enddate + "'"; // and app.\"Referral Code\" is not null ";

		if (applicationnumber != null && applicationnumber != "") {
			querysubmission += " and app.\"Application Number\" in ('" + applicationnumber + "') ";
		}

		Query q = entityManager.createNativeQuery(querysubmission, Tuple.class);

		List<Tuple> fetchapplication = q.getResultList();

		int targetSize = 1000;
		List<List<Tuple>> output = chopped(fetchapplication, targetSize);

		List<ObjectNode> returnlist = new ArrayList<>();
		// ObjectNode SUBMISSIONS=new ObjectNode();
		int fileNo = 1;
		for (List<Tuple> results : output) {
			List<ObjectNode> json = _toJson(results);
			for (int i = 0; i < json.size(); i++) {
				String appNo = json.get(i).get("Application Number").asText();
				appList.add(appNo);
				String custNo = json.get(i).get("Customer Number").asText();
				System.out.println(appNo + "                    " + custNo);

				q = entityManager.createNativeQuery(
						"select  case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'\r\n"
								+ " and \"Identification Type\" is not null and \"Application Number\" in ('" + appNo
								+ "')",
						Tuple.class);
				List<Tuple> mainApplicant = q.getResultList();

				if (mainApplicant != null && !mainApplicant.isEmpty()) {
					List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
					for (int j = 0; j < mainApplicantjson.size(); j++) {
						System.out.println(custNo);
						q = entityManager.createNativeQuery(
								"select  COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						List<Tuple> mainApplicantRA = q.getResultList();
						List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
						mainApplicantjson.get(j).put("MA_CA", mainApplicantRAJsom.get(0));

						q = entityManager.createNativeQuery(
								"select  COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						mainApplicantRAJsom = _toJson(mainApplicantRA);
						mainApplicantjson.get(j).put("MA_PMA", mainApplicantRAJsom.get(0));

						q = entityManager.createNativeQuery(
								"select adds.\"Mobile No\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Mobile No\" is not null and app.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						mainApplicantRAJsom = _toJson(mainApplicantRA);
						mainApplicantjson.get(j).put("MA_MT", mainApplicantRAJsom.get(0));

						q = entityManager.createNativeQuery(
								"select  adds.\"Bank Name\" BNK_NM,adds.\"Bank Account No\" ACC_NO from NEO_CAS_LMS_SIT1_SH.\"Bank Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Bank Account No\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						mainApplicantRAJsom = _toJson(mainApplicantRA);
						mainApplicantjson.get(j).put("MA_BNK", mainApplicantRAJsom.get(0));

						q = entityManager.createNativeQuery(
								"select  adds.\"Employer Name\" ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
										+ "when UPPER(adds.\"Industry\") like 'CONSTRUCTION' then 'CONSTRUCTION'\r\n"
										+ "when UPPER(adds.\"Industry\") like 'EDUCATION' then 'EDUCATION'\r\n"
										+ "when adds.\"Industry\" like 'Entertainment' then 'ENTERTAINMENT'\r\n"
										+ "when adds.\"Industry\" like 'Financial Services' then 'FINANCIAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Food and Food Processing' then 'FOOD'\r\n"
										+ "when replace(adds.\"Industry\",'&','') like 'GEMS  JEWELLERY' then 'GEMS AND JEWELLERY'\r\n"
										+ "when adds.\"Industry\" like 'Health Care' then 'HEALTHCARE'\r\n"
										+ "when adds.\"Industry\" like 'Hospitality' then 'HOSPITALITY AND TOURISM'\r\n"
										+ "when adds.\"Industry\" like 'Legal services' then 'LEGAL SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Manufacturing Unit' then 'MANAFACTURING'\r\n"
										+ "when adds.\"Industry\" like 'Media' then 'MEDIA AND ADVERTISING'\r\n"
										+ "when adds.\"Industry\" like 'Others' then 'OTHER'\r\n"
										+ "when adds.\"Industry\" like 'REAL ESTATE' then 'REAL ESTATE'\r\n"
										+ "when adds.\"Industry\" like 'Retail Shop' then 'RETAIL'\r\n"
										+ "when adds.\"Industry\" like 'Sales and E-Commerce Marketing' then 'SALES'\r\n"
										+ "when adds.\"Industry\" like 'Service Industry' then 'SERVICES'\r\n"
										+ "when adds.\"Industry\" like 'Software' then 'INFORMATION TECHNOLOGY'\r\n"
										+ "when adds.\"Industry\" like 'Sports' then 'SPORTS AND LEISURE'\r\n"
										+ "when adds.\"Industry\" like 'TEXTILES' then 'TEXTILES'\r\n"
										+ "when adds.\"Industry\" like 'Truck Driver and Transport' then 'TRANSPORT AND LOGISTICS'\r\n"
										+ "when adds.\"Industry\" like 'Tours And Travel' then 'TRAVEL' else 'OTHER'\r\n"
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\"='Salaried' and adds.\"Employer Name\" is not null and app.\"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();
						mainApplicantRAJsom = _toJson(mainApplicantRA);

						/*
						 * q = entityManager.createNativeQuery(
						 * "select  adds.\"Bank Name\" BNK_NM,adds.\"Bank Account No\" ACC_NO from NEO_CAS_LMS_SIT1_SH.\"Bank Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where  app.\"Application Number\" in ('"
						 * +appNo+"')",Tuple.class); List<Tuple> mainApplicantBA = q.getResultList();
						 * List<ObjectNode> mainApplicantBAJson = _toJson(mainApplicantBA);
						 * mainApplicantRAJsom.get(0).put("MA_EMP_AD",
						 * mainApplicantBAJson.get(0).toString());
						 */
						if (!mainApplicantRAJsom.isEmpty()) {
							mainApplicantjson.get(j).put("MA_EMP", mainApplicantRAJsom.get(0));
						}

					}

					json.get(i).put("MA", mainApplicantjson.get(0));

				}

				q = entityManager.createNativeQuery(
						"select  \"Customer Number\",case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\r\n"
								+ "\"First Name\" FST_NME, \"Middle Name\" MID_NME,\"Last Name\" LST_NME ,to_char(\"Date Of Birth\",'YYYY-MM-DD') DOB,\"Age\" AGE,case when UPPER(\"Gender\") like 'MALE' then UPPER(\"Gender\") when UPPER(\"Gender\") like 'FEMALE' then UPPER(\"Gender\") when UPPER(\"Gender\") is null then 'UNKNOWN' else 'OTHER' end GNDR\r\n"
								+ " from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Co-Applicant'\r\n"
								+ " and \"Application Number\" in ('" + appNo + "')",
						Tuple.class);
				List<Tuple> jointApplicant = q.getResultList();

				if (jointApplicant != null && !jointApplicant.isEmpty()) {

					List<ObjectNode> jointApplicantjson = _toJson(jointApplicant);

					for (int j = 0; j < jointApplicantjson.size(); j++) {
						System.out.println("jointApplicantjson index- " + j + " --  " + custNo);
						custNo = jointApplicantjson.get(j).get("Customer Number").asText();
						q = entityManager.createNativeQuery(
								"select  COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						List<Tuple> jointApplicantRA = q.getResultList();
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantjson.get(j).put("JA_CA", jointApplicantRAJsom.get(0));
						}

						jointApplicantjson.get(j).remove("Customer Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("JA", array);
					json.get(i).set("JAS", result);

				}

				q = entityManager.createNativeQuery(
						"select par.\"Code\",par.\"Name\",par.\"Address\" from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"Code\") where  app.\"Application Number\" in ('"
								+ appNo + "')",
						Tuple.class);
				List<Tuple> broker = q.getResultList();
				if (broker != null && !broker.isEmpty()) {
					List<ObjectNode> brokerjson = _toJson(broker);
					for (int j = 0; j < brokerjson.size(); j++) {
						String refcode = brokerjson.get(j).get("CODE").asText();
						System.out.println(refcode);
						q = entityManager.createNativeQuery(
								"select par.\"Addres\" \"ADD\",br.\"Branch City\" CTY,br.\"Branch State\" STE,br.\"Branch Pincode\" PIN,br.\"Branch Country\" CTRY from (select \"Referral Code\",\"Application Number\" from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD where \"Referral Code\" is not null) app left join NEO_CAS_LMS_SIT1_SH.\"BUSINESS PARTNER\" par on (app.\"Referral Code\"=par.\"CODE\") left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (par.\"DSA Branch\"=br.\"Branch Name\") where  app.\"Application Number\" in ('"
										+ appNo + "') and app.\"Referral Code\" in ('" + refcode + "')",
								Tuple.class);
						List<Tuple> brokerAddress = q.getResultList();
						List<ObjectNode> brokerAddressJsom = _toJson(brokerAddress);
						brokerjson.get(j).put("BR_ADD", brokerAddressJsom.get(0));

					}

					json.get(i).put("BR", brokerjson.get(0));
				}
				// objNode.put(propertyName, value)
			}

			ObjectNode root = mapper.createObjectNode();
			ObjectNode batch = mapper.createObjectNode();
			ObjectNode header = mapper.createObjectNode();
			header.put("COUNT", results.size());
			header.put("ORIGINATOR", "SHDFC");
			batch.put("HEADER", header);

			for (int js = 0; js < json.size(); js++) {
				json.get(js).remove("Application Number");
				json.get(js).remove("Customer Number");

			}
			returnlist.add(root);
			ArrayNode array = mapper.valueToTree(json);
			// batch.putArray("SUBMISSION").add(array);
			JsonNode result = mapper.createObjectNode().set("SUBMISSION", array);
			batch.put("SUBMISSIONS", result);
			root.set("BATCH", batch);
			ObjectMapper xmlMapper = new XmlMapper();

			try {
				String xml = xmlMapper.writeValueAsString(batch);
				xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
				xml = xml.replace("<JAS>", "").replace("</JAS>", "");

				// System.out.println(doc.getChildNodes().toString());

				String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
						+ xml + "</BATCH>";
				System.out.println(createXml);

				String filepath = "";// stringToDom(createXml, 1);

				GeneratedKeyHolder holder = new GeneratedKeyHolder();
				osourceTemplate.update(new PreparedStatementCreator() {
					@Override
					public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
						PreparedStatement statement = con.prepareStatement(
								"INSERT INTO hunter_job (emailsend, createon, jobfile, jobdataxml, emailsendon) VALUES (?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP) ",
								Statement.RETURN_GENERATED_KEYS);
						statement.setString(1, "1");
						statement.setString(2, filepath);
						statement.setString(3, createXml);
						return statement;
					}
				}, holder);

				long primaryKey = holder.getKey().longValue();

				String sql = "INSERT INTO `hunter_job_application` (`createon`, `applicationnumber`, `hunter_job_id`) VALUES (CURRENT_TIMESTAMP, ?, ?)";

				List<Object[]> parameters = new ArrayList<Object[]>();

				for (String cust : appList) {
					parameters.add(new Object[] { cust, primaryKey });
				}
				osourceTemplate.batchUpdate(sql, parameters);
				System.out.println(createXml);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} /*
				 * catch (SAXException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); } catch (ParserConfigurationException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */ catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} /*
				 * catch (TransformerException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); }
				 */
			fileNo++;

		}

		return returnlist;

	}

	private List<ObjectNode> _toJson(List<Tuple> results) {

		List<ObjectNode> json = new ArrayList<ObjectNode>();

		ObjectMapper mapper = new ObjectMapper();

		for (Tuple t : results) {
			List<TupleElement<?>> cols = t.getElements();

			ObjectNode one = mapper.createObjectNode();

			for (TupleElement col : cols) {
				if (col != null && col.getAlias() != null && t.get(col.getAlias()) != null) {
					one.put(col.getAlias(), t.get(col.getAlias()).toString());
				}
			}

			json.add(one);
		}

		return json;
	}

	public boolean stringToDomAdhoc(String xmlSource, String fileNo, String hour)
			throws SAXException, ParserConfigurationException, IOException, TransformerException {

		boolean send = false;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xmlSource)));

		// Use a Transformer for output
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer = tFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(fileNo));
		transformer.transform(source, result);
		try {

			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(CURDATE(),'%d-%m-%Y') todate,DATE_FORMAT(date(CURDATE()-1),'%d-%m-%Y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));

			// sendEmail(fileNo, hour);

			String toemail = "vijay.uniyal@shubham.co";
			String subject = "";
			String bodypart = "";
			if (hour.contains("AM")) {
				subject = "Hunter upload data file – " + mailconfig.getFromdate() + " 16:00:00 to "
						+ mailconfig.getTodate() + " 08:59:59" + " - " + fileNo.replace(".xml", "");
				bodypart = mailconfig.getFromdate() + " 16:00:00 to " + mailconfig.getTodate() + " 08:59:59";
				System.out.println(mailconfig.getFromdate() + " 16:00:00 to " + mailconfig.getTodate() + " 08:59:59");
			} else {
				subject = "Hunter upload data file – " + mailconfig.getTodate() + " 09:00:00 to "
						+ mailconfig.getTodate() + " 15:59:59" + " - " + fileNo.replace(".xml", "");
				bodypart = mailconfig.getTodate() + " 09:00:00 to " + mailconfig.getTodate() + " 15:59:59";
			}

			String body = "<html><body><span>Dear Sir/Madam</span><br/><br/><span>May please find attached herewith Hunter upload data files in xls and xml format for the period - "
					+ bodypart
					+ "<span><br/><br/><span>Regards</span><br/><span>IT Support/IT team</span><body></html>";
			Email sendemail = new Email(mailconfig.getEmailto(), subject, body, fileNo, mailconfig.getSmtphost(),
					mailconfig.getSmtpport(), mailconfig.getUsername(), mailconfig.getPassword());
			//Thread emailThread = new Thread(sendemail);
			//emailThread.start();

			send = true;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return send;

	}
	public boolean stringToDom(String xmlSource, String fileNo, String hour)
			throws SAXException, ParserConfigurationException, IOException, TransformerException {

		boolean send = false;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xmlSource)));

		// Use a Transformer for output
		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer = tFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

		DOMSource source = new DOMSource(doc);
		StreamResult result = new StreamResult(new File(fileNo));
		transformer.transform(source, result);
		try {

			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(CURDATE(),'%d-%m-%Y') todate,DATE_FORMAT(date(CURDATE()-1),'%d-%m-%Y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));

			// sendEmail(fileNo, hour);

			String toemail = "vijay.uniyal@shubham.co";
			String subject = "";
			String bodypart = "";
			if (hour.contains("AM")) {
				subject = "Hunter upload data file – " + mailconfig.getFromdate() + " 16:00:00 to "
						+ mailconfig.getTodate() + " 08:59:59" + " - " + fileNo.replace(".xml", "");
				bodypart = mailconfig.getFromdate() + " 16:00:00 to " + mailconfig.getTodate() + " 08:59:59";
				System.out.println(mailconfig.getFromdate() + " 16:00:00 to " + mailconfig.getTodate() + " 08:59:59");
			} else {
				subject = "Hunter upload data file – " + mailconfig.getTodate() + " 09:00:00 to "
						+ mailconfig.getTodate() + " 15:59:59" + " - " + fileNo.replace(".xml", "");
				bodypart = mailconfig.getTodate() + " 09:00:00 to " + mailconfig.getTodate() + " 15:59:59";
			}

			String body = "<html><body><span>Dear Sir/Madam</span><br/><br/><span>May please find attached herewith Hunter upload data files in xls and xml format for the period - "
					+ bodypart
					+ "<span><br/><br/><span>Regards</span><br/><span>IT Support/IT team</span><body></html>";
			Email sendemail = new Email(mailconfig.getEmailto(), subject, body, fileNo, mailconfig.getSmtphost(),
					mailconfig.getSmtpport(), mailconfig.getUsername(), mailconfig.getPassword());
			Thread emailThread = new Thread(sendemail);
			emailThread.start();

			send = true;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return send;

	}

	static <T> List<List<T>> chopped(List<T> list, final int L) {
		List<List<T>> parts = new ArrayList<List<T>>();
		final int N = list.size();
		for (int i = 0; i < N; i += L) {
			parts.add(new ArrayList<T>(list.subList(i, Math.min(N, i + L))));
		}
		return parts;
	}

	public void sendSEmails(String path, String hour) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		MimeMessage message = javaMailSender.createMimeMessage();
		MimeMessageHelper helper = new MimeMessageHelper(message, true);
		helper.setFrom("alerts.etl@shubham.co");
		helper.setTo("vijay.uniyal@shubham.co");
		if (hour.contains("AM")) {
			helper.setSubject("Hunter upload data file – "
					+ sdf.format((new Date((new Date()).getTime() - 10 * 3600 * 3600))) + " 16:00:00 to "
					+ sdf.format(new Date()) + " 08:59:59" + " - " + path.replace(".xml", ""));
			System.out.println(sdf.format((new Date((new Date()).getTime() - 10 * 3600 * 3600))) + " 16:00:00 to "
					+ sdf.format(new Date()) + " 08:59:59");
		} else {
			helper.setSubject("Hunter upload data file – " + sdf.format(new Date()) + " 09:00:00 to "
					+ sdf.format(new Date()) + " 15:59:59" + " - " + path.replace(".xml", ""));
		}

		helper.setText(
				"<html><body><h1>Dear Sir/Madam</h1></br><span>May please find attached herewith Hunter upload data files in xls and xml format for the period - 28-12-2021 16:00:00 to 29-12-2021 08:59:59 File No 000001<span></br><span>Regards</span></br><span>IT Support/IT team</span><body></html>",
				true);
		FileSystemResource file = new FileSystemResource(new File(path));
		helper.addAttachment(file.getFilename(), file);
		javaMailSender.send(message);
	}

	private void writeHeaderLine(List<Tuple> results, XSSFSheet sheet) throws SQLException {

		headerValues = new ArrayList();
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
				if (t.get(headerValues.get(p)) != null) {

					row.createCell((short) p).setCellValue(t.get(headerValues.get(p)).toString());
				}

			}

		}

	}

}
