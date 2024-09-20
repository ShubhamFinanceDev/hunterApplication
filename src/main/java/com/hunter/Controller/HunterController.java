package com.hunter.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
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

import org.apache.commons.collections4.ListUtils;
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

	/////////////////////////////////// REJECTION CASES
/////////////////////////////////// START/////////////////////////////////////////////
// @Scheduled(cron = "0 0 9,16 * * ?")
	// @Scheduled(cron = "0 30 22 * * ?")
	@GetMapping("/fetchNRData")
	public List<ObjectNode> fetchNRData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
// hour="AM";
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;

			querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'DECLINED' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,case when app.\"Product Type Code\" like 'HL' then 'HL_IND' else 'NHL_IND' end PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
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
					+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Current Status\" in ('CANCELLATION','REJECTION') and app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null) and app.\"Application Number\" in ('APPL05000396','APPL05001057','APPL05001395','APPL05002386','APPL05002943','APPL05002963','APPL05003898','APPL05004003','APPL05004759','APPL05005385','APPL05006289','APPL05007445','APPL05008175','APPL05008476','APPL05008962','APPL05009884','APPL05009980','APPL05010113','APPL05011440','APPL05012115','APPL05013558','APPL05013651','APPL05013873','APPL05014873','APPL05015249','APPL05016081','APPL05016113','APPL05016590','APPL05016753','APPL05017968','APPL05018012','APPL05019172','APPL05019354','APPL05020127','APPL05022497','APPL05022699','APPL05023275','APPL05024035','APPL05024073','APPL05024452','APPL05024485','APPL05025045','APPL05025052','APPL05025116','APPL05025904','APPL05026191','APPL05026211','APPL05026413','APPL05026554','APPL05027527','APPL05027579','APPL05027956','APPL05028582','APPL05028646','APPL05028759','APPL05028857','APPL05029278','APPL05029536','APPL05029538','APPL05029565','APPL05030414','APPL05031019','APPL05031092','APPL05031436','APPL05031463','APPL05031672','APPL05031828','APPL05031852','APPL05032285','APPL05032786','APPL05033280','APPL05033373','APPL05033913','APPL05033929','APPL05034039','APPL05034216','APPL05034278','APPL05034392','APPL05034577','APPL05034669','APPL05035624','APPL05036436','APPL05036454','APPL05036536','APPL05036619','APPL05036895','APPL05037379','APPL05037528','APPL05037891','APPL05037893','APPL05037951','APPL05038326','APPL05038581','APPL05038851','APPL05039959','APPL05040119','APPL05040429','APPL05041390','APPL05041542','APPL05041819','APPL05041847','APPL05042067','APPL05042223','APPL05042236','APPL05043024','APPL05043351','APPL05043431','APPL05043686','APPL05043738','APPL05043748','APPL05043762','APPL05043978','APPL05044021','APPL05044201','APPL05044320','APPL05044350','APPL05044491','APPL05044644','APPL05044650','APPL05044674','APPL05044788','APPL05044897','APPL05044955','APPL05044969','APPL05044979','APPL05045282','APPL05045296','APPL05045475','APPL05045493','APPL05045690','APPL05045931','APPL05046073','APPL05046082','APPL05046154','APPL05046255','APPL05046294','APPL05046303','APPL05046344','APPL05046579','APPL05046585','APPL05046614','APPL05046672','APPL05046673','APPL05046694','APPL05046717','APPL05046873','APPL05046998','APPL05047016','APPL05047142','APPL05047210','APPL05047330','APPL05047339','APPL05047483','APPL05047489','APPL05047538','APPL05047891','APPL05047900','APPL05048003','APPL05048005','APPL05048978','APPL05048993','APPL05049178','APPL05049279','APPL05049330','APPL05049440','APPL05049452','APPL05049527','APPL05049574','APPL05049793','APPL05049979','APPL05050016','APPL05050081','APPL05050096','APPL05050099','APPL05050105','APPL05050299','APPL05050306','APPL05050498','APPL05050526','APPL05050611','APPL05050629','APPL05050651','APPL05050669','APPL05050671','APPL05050933','APPL05050942','APPL05051130','APPL05051159','APPL05051274','APPL05051289','APPL05051305','APPL05051519','APPL05051523','APPL05051554','APPL05051671','APPL05051732','APPL05051794','APPL05052072','APPL05052188','APPL05052189','APPL05052409','APPL05052471','APPL05052495','APPL05052527','APPL05052771','APPL05052882','APPL05052893','APPL05052905','APPL05053006','APPL05053011','APPL05053033','APPL05053068','APPL05053187','APPL05053205','APPL05053231','APPL05053394','APPL05053417','APPL05053426','APPL05053782','APPL05053797','APPL05053822','APPL05053880','APPL05053899','APPL05053994','APPL05054009','APPL05054065','APPL05054181','APPL05054190','APPL05054256','APPL05054465','APPL05054633','APPL05054908','APPL05054951','APPL05054956','APPL05055136','APPL05055167','APPL05055185','APPL05055378','APPL05055540','APPL05055542','APPL05055683','APPL05055877','APPL05055924','APPL05056000','APPL05056075','APPL05056416','APPL05056420','APPL05056479','APPL05056481','APPL05056571','APPL05056873','APPL05057094','APPL05057128','APPL05057146','APPL05057147','APPL05057159','APPL05057360','APPL05057490','APPL05057491','APPL05057586','APPL05057890','APPL05058025','APPL05058119','APPL05058314','APPL05058404','APPL05058560','APPL05058692','APPL05058705','APPL05058799','APPL05059046','APPL05059064','APPL05059100','APPL05059202','APPL05059310','APPL05059628','APPL05059707','APPL05059923','APPL05060154','APPL05060218','APPL05060288','APPL05060667','APPL05060671','APPL05060740','APPL05060756','APPL05060986','APPL05061115','APPL05061365','APPL05061661','APPL05062709','APPL05062888','APPL05062891','APPL05062925','APPL05062972','APPL05063123','APPL05063167','APPL05063214','APPL05063403','APPL05063698','APPL05063804','APPL05063973','APPL05064297','APPL05064367','APPL05064553','APPL05064741','APPL05064797','APPL05064904','APPL05065007','APPL05065136','APPL05065503','APPL05065514','APPL05065655','APPL05065660','APPL05065690','APPL05065699','APPL05065712','APPL05065864','APPL05065881','APPL05065902','APPL05065933','APPL05066003','APPL05066308','APPL05066359','APPL05066413','APPL05066415','APPL05066434','APPL05066454','APPL05066470','APPL05066609','APPL05066727','APPL05066833','APPL05067391','APPL05067443','APPL05067495','APPL05067639','APPL05067655','APPL05067662','APPL05067743','APPL05067765','APPL05067793','APPL05067940','APPL05067994','APPL05068017','APPL05068076','APPL05068086','APPL05068294','APPL05068307','APPL05068489','APPL05068555','APPL05068562','APPL05068590','APPL05068638','APPL05068671','APPL05068681','APPL05068683','APPL05068690','APPL05069169','APPL05069187','APPL05069306','APPL05069445','APPL05069505','APPL05069889','APPL05069916','APPL05069997','APPL05070488','APPL05070494','APPL05070542','APPL05070570','APPL05070594','APPL05070702','APPL05070719','APPL05070742','APPL05070792','APPL05071017','APPL05071094','APPL05071694','APPL05071697','APPL05071734','APPL05071765','APPL05071790','APPL05071811','APPL05071861','APPL05072075','APPL05072219','APPL05072367','APPL05072416','APPL05072446','APPL05072577','APPL05072608','APPL05072632','APPL05072811','APPL05072896','APPL05073027','APPL05073414','APPL05073425','APPL05073427','APPL05073489','APPL05073491','APPL05073516','APPL05073523','APPL05073727','APPL05073736','APPL05073971','APPL05074019','APPL05074047','APPL05074097','APPL05074150','APPL05074293','APPL05074301','APPL05074317','APPL05074354','APPL05074365','APPL05074627','APPL05074928','APPL05074942','APPL05075099','APPL05075371','APPL05075498','APPL05075582','APPL05075889','APPL05076233','APPL05076308','APPL05076342','APPL05076368','APPL05076431','APPL05076599','APPL05076600','APPL05076646','APPL05076752','APPL05076769','APPL05076899','APPL05076905','APPL05077102','APPL05077517','APPL05077523','APPL05077689','APPL05077799','APPL05077937','APPL05077943','APPL05077970','APPL05078012','APPL05078934','APPL05078971','APPL05079165','APPL05079333','APPL05079335','APPL05079384','APPL05079403','APPL05079537','APPL05079549','APPL05079602','APPL05079696','APPL05079773','APPL05079787','APPL05080211','APPL05081200','APPL05081275','APPL05081853','APPL05082043','APPL05082103','APPL05082442','APPL05082443','APPL05082939','APPL05083195','APPL05083287','APPL05083380','APPL05083620','APPL05083654','APPL05083672','APPL05083817','APPL05084108','APPL05084358','APPL05084513','APPL05084551','APPL05084634','APPL05084819','APPL05084998','APPL05085129','APPL05085305','APPL05085385','APPL05086091','APPL05086177','APPL05086357','APPL05086378','APPL05086400','APPL05086449','APPL05086464','APPL05086888','APPL05087251','APPL05087558','APPL05087722','APPL05087793','APPL05088108','APPL05088188','APPL05088313','APPL05088325','APPL05088335','APPL05088688','APPL05088882','APPL05088987','APPL05089266','APPL05090319','APPL05090700','APPL05091302','APPL05091611','APPL05091900','APPL05093859','APPL05093903','APPL05093994','APPL05094030','APPL05094278','APPL05096672','APPL05097941','APPL05097959','APPL05099111','APPL05099214','APPL05099544','APPL05100443','APPL05101657','APPL05104598','APPL05105352','APPL05106403','APPL05107424','APPL05107716','APPL05107940','APPL05108238','APPL05108247','APPL05110789','APPL05111342','APPL05111809','APPL05111827','APPL05111928','APPL05112295','APPL05113421','APPL05114090','APPL05114696','APPL05115407','APPL05115546','APPL05115598','APPL05116092','APPL05117615','APPL05118070','APPL05119043','APPL05119829','APPL05121545','APPL05124323','APPL05124809','APPL05125674','APPL05126344','APPL05126827','APPL05127431','APPL05128072','APPL05128482','APPL05128926','APPL05128966','APPL05128967','APPL05129108','APPL05129551','APPL05129685','APPL05129721','APPL05130470','APPL05131302','APPL05131448','APPL05131627','APPL05132042','APPL05132054','APPL05132103','APPL05132170','APPL05132346','APPL05132439','APPL05134097','APPL05134486','APPL05134541','APPL05134640','APPL05135157','APPL05135229','APPL05135383','APPL05135639','APPL05136059','APPL05136532','APPL05136916','APPL05137016','APPL05137023','APPL05137314','APPL05137325','APPL05138244','APPL05138862','APPL05139121','APPL05139537','APPL05140330','APPL05141456','APPL05141629','APPL05142445','APPL05142816','APPL05143070','APPL05143305','APPL05143929','APPL05143982','APPL05145120','APPL05145221','APPL05145245','APPL05145536','APPL05146449','APPL05146823','APPL05146833','APPL05146871','APPL05147218','APPL05147571','APPL05147680','APPL05147775','APPL05147781','APPL05148302','APPL05148366','APPL05148420','APPL05148492','APPL05148724','APPL05148762','APPL05149189','APPL05149379','APPL05149764','APPL05149813','APPL05149872','APPL05150022','APPL05150071','APPL05150127','APPL05150511','APPL05150615','APPL05151011','APPL05151112','APPL05151250','APPL05151324','APPL05152211','APPL05152231','APPL05152349','APPL05152521','APPL05152612','APPL05152657','APPL05153012','APPL05153024','APPL05153035','APPL05153051','APPL05153079','APPL05153080','APPL05153332','APPL05153377','APPL05153551','APPL05153823','APPL05154824','APPL05155214','APPL05155310','APPL05156816','APPL05157113','APPL05157417','APPL05158716','APPL05158820','APPL05158856','APPL05159110','APPL05159373','APPL05159618','APPL05159944','APPL05159946','APPL05160518','APPL05160918','APPL05160926','APPL05161613','APPL05162217','APPL05162229','APPL05162286','APPL05162313','APPL05162377','APPL05162533','APPL05163003','APPL05163565','APPL05163567','APPL05164421','APPL05164429','APPL05164714','APPL05164834','APPL05164937','APPL05165144','APPL05165519','APPL05165743','APPL05165922','APPL05166155','APPL05166277','APPL05166524','APPL05166537','APPL05166604','APPL05166683','APPL05166849','APPL05166854','APPL05167392','APPL05167420','APPL05167594','APPL05167711','APPL05167887','APPL05168094','APPL05168532','APPL05168533','APPL05169316','APPL05169518','APPL05169710','APPL05169911','APPL05170029','APPL05170044','APPL05170173','APPL05170196','APPL05170311','APPL05170377','APPL05170660','APPL05170962','APPL05171012','APPL05171099','APPL05171231','APPL05171258','APPL05171278','APPL05171302','APPL05171643','APPL05171838','APPL05172032','APPL05172230','APPL05172249','APPL05172289','APPL05172502','APPL05172512','APPL05172636','APPL05172670','APPL05172676','APPL05172829','APPL05172834','APPL05172855','APPL05173228','APPL05173317','APPL05173334','APPL05173576','APPL05174048','APPL05174510','APPL05174648','APPL05174711','APPL05174720','APPL05174797','APPL05174838','APPL05174943','APPL05174959','APPL05175226','APPL05175431','APPL05175437','APPL05175540','APPL05175959','APPL05175997','APPL05176046','APPL05176112','APPL05176215','APPL05176225','APPL05176254','APPL05176313','APPL05176626','APPL05176641','APPL05176918','APPL05176961','APPL05177151','APPL05177737','APPL05177813','APPL05177875','APPL05177997','APPL05178173','APPL05178323','APPL05178413','APPL05178537','APPL05178813','APPL05179027','APPL05179222','APPL05179223','APPL05179225','APPL05179234','APPL05179773','APPL05180117','APPL05180632','APPL05180697','APPL05181076','APPL05181230','APPL05181317','APPL05181655','APPL05181672','APPL05181786','APPL05181901','APPL05182158','APPL05182486','APPL05182492','APPL05182685','APPL05183052','APPL05183058','APPL05183462','APPL05183617','APPL05183629','APPL05183725','APPL05184042','APPL05184121','APPL05184144','APPL05184163','APPL05184234','APPL05184286','APPL05184421','APPL05184859','APPL05185657','APPL05185767','APPL05185965','APPL05187012','APPL05187069','APPL05187138','APPL05187448','APPL05187469','APPL05187537','APPL05188152','APPL05188332','APPL05188611','APPL05188767','APPL05188915','APPL05188972','APPL05189231','APPL05189304','APPL05189916','APPL05190151','APPL05190410','APPL05190443','APPL05190901','APPL05190934','APPL05190953','APPL05191160','APPL05191667','APPL05191672','APPL05191695','APPL05191820','APPL05192253','APPL05192255','APPL05192556','APPL05192832','APPL05193022','APPL05193239','APPL05193323','APPL05194251','APPL05194793','APPL05195017','APPL05195543','APPL05195559','APPL05195667','APPL05195961','APPL05196717','APPL05199310','APPL05199448','APPL05203160','APPL05203522','APPL05204985','APPL05205197','APPL05205511','APPL05206509','APPL05206913','APPL05207866','APPL05208360','APPL05208381','APPL05209216','APPL05210827')";

			String dbapp = "SELECT APPLICATION_NUMBER from rcu_nr_cases";

			List<String> dbapplist = null;// osourceTemplate.queryForList(dbapp, String.class);

			if (dbapplist != null && dbapplist.size() > 0) {

				List<List<String>> partitions = ListUtils.partition(dbapplist, 999);
				System.out.println(partitions.size());

				for (int p = 0; p < partitions.size(); p++) {
					querysubmission += " and app.\"Application Number\" in ("
							+ partitions.get(p).stream().collect(Collectors.joining("','", "'", "'")) + ")";

				}
				q = entityManager.createNativeQuery(querysubmission, Tuple.class);

			} else {
				q = entityManager.createNativeQuery(querysubmission, Tuple.class);
			}
			System.out.println(querysubmission);

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
								+ " and  \"First Name\" is not null and \"Application Number\" in ('" + appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
						}

						mainApplicantRAJsom = _toJson(mainApplicantRA);
						if (mainApplicantRAJsom != null && mainApplicantRAJsom.size() > 0) {
							prefix = "MA_PA";
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
							mainApplicantjson.get(mainapp).put("MA_PMA", mainApplicantRAJsom.get(0));
						}

/////////////////////////// Main Application Permanant Application End
/////////////////////////// ///////////////////////////////////////////////////////////////
///////////////////////////// Main Application Property Application
///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
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
								"select  app.\"Application Number\",case when adds.\"Occupation Type\" like 'Self Employed' then adds.\"Organization Name\" else adds.\"Employer Name\" end ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
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
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and (adds.\"Organization Name\" is not null or adds.\"Employer Name\" is not null) and app.\"Application Number\" in ('"
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

								// Employer Address
								q = entityManager.createNativeQuery(
										"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								if (employeraddressJsom != null && employeraddressJsom.size() > 0) {
									mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
									prefix = prefix + "_EMP";
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(
												employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
										if (employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
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
								if (employerTelephone != null && employerTelephone.size() > 0) {
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")));
										if (employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
									}
									mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
								}
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
								+ " and \"Application Number\" in ('" + appNo
								+ "') and \"Customer Number\" is not null and rownum<4",
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4 ",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4 ",
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
			header.put("SUPPRESS", "Y");
			batch.put("HEADER", header);

			ObjectNode subheader = mapper.createObjectNode();
			ObjectNode eventheader = mapper.createObjectNode();

			ObjectNode evendata = mapper.createObjectNode();
			evendata.put("CODE", "NC1");
			evendata.put("OPERATION", "I");
			eventheader.put("EVENT", evendata);
			subheader.put("EVENTS", eventheader);
			ObjectNode notesheader = mapper.createObjectNode();
			eventheader.put("NOTE", fetchapplication.size());
			subheader.put("NOTES", "SHDFC");
			batch.put("SUB_HEADER", subheader);

			for (int js = 0; js < json.size(); js++) {
				json.get(js).remove("Application Number");
				json.get(js).remove("Customer Number");
				json.get(js).remove("Neo CIF ID");

			}
			ArrayNode array = mapper.valueToTree(json);
			JsonNode result = mapper.createObjectNode().set("SUBMISSION", array);
			batch.put("SUBMISSIONS", result);
			root.set("BATCH", batch);
			System.out.println(batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);

//////////////////////////// GENERATE XML FILE and Send Email

////////// GENERATE XLS LOGIC/////////////////////////////////////////////

			if (fetchapplication != null && fetchapplication.size() > 0) {

				FileOutputStream fileOut = new FileOutputStream("RCU_NR" + ".xlsx");
				workbook.write(fileOut);
				fileOut.close();

				System.out.println("file generated successfully");

/// GENERATE XML AND SEND EMAIL//////////////////////////////
				boolean filepath = stringToDom(createXml, "RCU_NR" + ".xml", hour);
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

	///////////////////////////////////// REJECTION CASES END
	//////////////////////////////////////////////// ADHOC DATA///////and
	//////////////////////////////////////////////// app.\"Sanction Date\" is not
	//////////////////////////////////////////////// null//////////////////////////////////////////////////
	@GetMapping("/adhocfetchData")
	public List<ObjectNode> adhocfetchData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
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
								+ " and \"Application Number\" in ('" + appNo + "')",
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
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
										"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			String sql = "SELECT `nextFileSequence`(1) from dual";

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

	/////////////////////////////////////////// SEM
	/////////////////////////////////////////// DATA//////////////////////////////////////////////////

	// @Scheduled(cron = "0 44 10 * * ?")
	@GetMapping("/fetchSMEData")
	public List<ObjectNode> fetchSMEData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MP_PAN",
				"MP_FST_NME", "MP_MID_NME", "MP_LST_NME", "MP_DOB", "MP_AGE", "MP_GNDR", "MP_NAT_CDE", "MP_PA_ADD",
				"MP_PA_CTY", "MP_PA_STE", "MP_PA_CTRY", "MP_PA_PIN", "MP_RA_ADD", "MP_RA_CTY", "MP_RA_STE",
				"MP_RA_CTRY", "MP_RA_PIN", "MP_PRE_ADD", "MP_PRE_CTY", "MP_PRE_STE", "MP_PRE_CTRY", "MP_PRE_PIN",
				"MP_HT_TEL_NO", "MP_M_TEL_NO", "SME_ORG_NME", "SME_EMP_IND", "SME_CONSTIT", "SME_TAN_NO", "SME_DAT_INC",
				"SME_CREG_NO", "SME_ORG_STDAT", "SME_SAL_TXN", "SME_TURNOV", "SME_EMP_NO", "SME_GST_NO", "SME_STATUS",
				"MAC_ADD_ADD", "MAC_ADD_CTY", "MAC_ADD_STE", "MAC_ADD_CTRY", "MAC_ADD_PIN", "MAC_ADD_STATUS",
				"MAC_TEL3_TEL_NO", "MAC_TEL3_EXT_NO", "MAC_TEL3_STATUS", "MAC_TEL1_TEL_NO", "MAC_TEL1_EXT_NO",
				"MAC_TEL1_STATUS", "MAC_TEL2_TEL_NO", "MAC_TEL2_EXT_NO", "MAC_TEL2_STATUS", "MAC_EMA2_ADD",
				"MAC_EMA2_CO_ADD", "MAC_EMA2_DO_NAM", "MAC_EMA2_STATUS", "MAC_EMA1_ADD", "MAC_EMA1_CO_ADD",
				"MAC_EMA1_DO_NAM", "MAC_EMA1_STATUS", "MP_EMA_ADD", "MP1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
				"MP_DOC_TYP", "MP_DOC_NO", "MP1_DOC_TYP", "MP1_DOC_NO", "MP2_DOC_TYP", "MP2_DOC_NO", "MP3_DOC_TYP",
				"MP3_DOC_NO", "MP4_DOC_TYP", "MP4_DOC_NO", "MP5_DOC_TYP", "MP5_DOC_NO", "MP6_DOC_TYP", "MP6_DOC_NO",
				"MP7_DOC_TYP", "MP7_DOC_NO", "MP8_DOC_TYP", "MP8_DOC_NO", "MP9_DOC_TYP", "MP9_DOC_NO", "MP_ORG_NME",
				"MP_EMP_IND", "MP1_ORG_NME", "MP1_EMP_IND", "MP_EMP_ADD", "MP_EMP_CTY", "MP_EMP_STE", "MP_EMP_CTRY",
				"MP_EMP_PIN", "MP_EMP_TEL", "MP1_EMP_ADD", "MP1_EMP_CTY", "MP1_EMP_STE", "MP1_EMP_CTRY", "MP1_EMP_PIN",
				"MP1_EMP_TEL", "CP_PAN", "CP_FST_NME", "CP_MID_NME", "CP_LST_NME", "CP_DOB", "CP_AGE", "CP_GNDR",
				"CP1_PAN", "CP1_FST_NME", "CP1_MID_NME", "CP1_LST_NME", "CP1_DOB_1", "CP1_AGE_1", "CP1_GNDR_1",
				"CP2_PAN", "CP2_FST_NME", "CP2_MID_NME", "CP2_LST_NME", "CP2_DOB", "CP2_AGE", "CP2_GNDR", " CP_RA_ADD",
				"CP_RA_CTY", "CP_RA_STE", "CP_RA_CTRY", "CP_RA_PIN", "CP1_RA_ADD", "CP1_RA_CTY", "CP1_RA_STE",
				"CP1_RA_CTRY", "CP1_RA_PIN", "CP2_RA_ADD", "CP2_RA_CTY", "CP2_RA_STE", "CP2_RA_CTRY", "CP2_RA_PIN",
				"CP_RA_DOC_TYP_1", "CP_RA_DOC_NO_1", "CP_RA_DOC_TYP_2", "CP_RA_DOC_NO_2", "CP_RA_DOC_TYP_3",
				"CP_RA_DOC_NO_3", "CP_RA_DOC_TYP_4", "CP_RA_DOC_NO_4", "CP_RA_DOC_TYP_5", "CP_RA_DOC_NO_5",
				"CP_RA_DOC_TYP_6", "CP_RA_DOC_NO_6", "CP_RA_DOC_TYP_7", "CP_RA_DOC_NO_7", "CP_RA_DOC_TYP_8",
				"CP_RA_DOC_NO_8", "CP_RA_DOC_TYP_9", "CP_RA_DOC_NO_9", "CP_RA_DOC_TYP_10", "CP_RA_DOC_NO_10",
				"CP1_RA_DOC_TYP_1", "CP1_RA_DOC_NO_1", "CP1_RA_DOC_TYP_2", "CP1_RA_DOC_NO_2", "CP1_RA_DOC_TYP_3",
				"CP1_RA_DOC_NO_3", "CP1_RA_DOC_TYP_4", "CP1_RA_DOC_NO_4", "CP1_RA_DOC_TYP_5", "CP1_RA_DOC_NO_5",
				"CP1_RA_DOC_TYP_6", "CP1_RA_DOC_NO_6", "CP1_RA_DOC_TYP_7", "CP1_RA_DOC_NO_7", "CP1_RA_DOC_TYP_8",
				"CP1_RA_DOC_NO_8", "CP1_RA_DOC_TYP_9", "CP1_RA_DOC_NO_9", "CP1_RA_DOC_TYP_10", "CP1_RA_DOC_NO_10",
				"CP2_RA_DOC_TYP_1", "CP2_RA_DOC_NO_1", "CP2_RA_DOC_TYP_2", "CP2_RA_DOC_NO_2", "CP2_RA_DOC_TYP_3",
				"CP2_RA_DOC_NO_3", "CP2_RA_DOC_TYP_4", "CP2_RA_DOC_NO_4", "CP2_RA_DOC_TYP_5", "CP2_RA_DOC_NO_5",
				"CP2_RA_DOC_TYP_6", "CP2_RA_DOC_NO_6", "CP2_RA_DOC_TYP_7", "CP2_RA_DOC_NO_7", "CP2_RA_DOC_TYP_8",
				"CP2_RA_DOC_NO_8", "CP2_RA_DOC_TYP_9", "CP2_RA_DOC_NO_9", "CP2_RA_DOC_TYP_10", "CP2_RA_DOC_NO_10",
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
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;
			if (hour.contains("AM")) {
				querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,'SME_ACC' PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
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
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null "
						+ " and app.VERDICT_DATE between TO_TIMESTAMP ('" + mailconfig.getFromdate()
						+ " 16:00:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('" + mailconfig.getTodate()
						+ " 08:59:59', 'DD-Mon-RR HH24:MI:SS') "
						+ "and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Applicant Type\"='Primary Applicant')";// and
				// app.\"Application
				// Number\"
				// in
				// ('APPL05199249','APPL05199693','APPL05198249','APPL05197802','APPL05197543','APPL05197551','APPL05192314','APPL05192622','APPL05192239','APPL05192240')
				// ";

			} else if (hour.contains("PM")) {
				querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,'SME_ACC' PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
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
						+ mailconfig.getTodate()
						+ " 15:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Applicant Type\"='Primary Applicant')";
			}
			System.out.println(querysubmission);
			String dbapp = "SELECT applicationnumber from hunter_job_nonindividual_application";

			List<String> dbapplist = osourceTemplate.queryForList(dbapp, String.class);
			// dbapplist = null;
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
						"select \"Application Number\", case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\"Institution Name\" FST_NME, null MID_NME,\"Institution Name\" LST_NME,\r\n"
								+ "                                to_char(\"INCORPORATION_DATE\",'YYYY-MM-DD') DOB from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Applicant Type\"='Primary Applicant' and \"Identification Type\" like 'PAN'\r\n"
								+ "								 and  \"Institution Name\" is not null and \"Application Number\" in ('"
								+ appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
					mainApplicantAll.addAll(mainApplicant);

					List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
					for (int mainapp = 0; mainapp < mainApplicantjson.size(); mainapp++) {
						String prefix = "MP";
						if (mainapp > 0) {
							prefix = "MP" + mainapp;
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
						if (mainApplicantRAJsom != null && mainApplicantRAJsom.size() > 0) {
							prefix = "MP_RA";
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
							mainApplicantjson.get(mainapp).put("MP_CA", mainApplicantRAJsom.get(0));
						}

						///////////////////////////// Main Application Residential Application End
						///////////////////////////// ///////////////////////////////////////////////////////////////

						///////////////////////////// Main Application Permanant Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
						}

						mainApplicantRAJsom = _toJson(mainApplicantRA);
						if (mainApplicantRAJsom != null && mainApplicantRAJsom.size() > 0) {
							prefix = "MP_PA";
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
							mainApplicantjson.get(mainapp).put("MP_PMA", mainApplicantRAJsom.get(0));
						}

						/////////////////////////// Main Application Permanant Application End
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
							mainApplicantjson.get(mainapp).put("MP_HT", mainApplicantRAJsom.get(0));
							prefix = "MP_HT";
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
							mainApplicantjson.get(mainapp).put("MP_MT", mainApplicantRAJsom.get(0));
							prefix = "MP_M";
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
							mainApplicantjson.get(mainapp).put("MP_EMA", mainApplicantRAJsom.get(0));
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

							mainApplicantjson.get(mainapp).put("MP_BNK", mainApplicantRAJsom.get(0));

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
								prefix = "MP";
								if (docid > 0) {
									prefix = "MP" + docid;
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
							JsonNode result = mapper.createObjectNode().set("MP_ID", array);
							mainApplicantjson.get(mainapp).set("MPDOC", result);
						}

						///////////////////// DOCUMENT ID END

					}
					json.get(app).put("MP", mainApplicantjson.get(0));
				}

				// Main Application Data Population End

				///////////////////////////////////// Company Details
				///////////////////////////////////// ////////////////////////////////////////////////

				q = entityManager.createNativeQuery(
						"select distinct \"Customer Number\",\"Institution Name\" ORG_NME,\"REGISTRATION_NUMBER\" TAN_NO from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Application Number\"='"
								+ appNo + "' and \"Applicant Type\"='Primary Applicant'",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> empjointApplicant = q.getResultList();
				String prefix;
				if (empjointApplicant != null && !empjointApplicant.isEmpty()) {
					jointApplicantAll.addAll(empjointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(empjointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "SME";
						if (ja > 0) {
							prefix = "SME" + ja;
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
						jointApplicantjson.get(ja).remove("Customer Number");
						q = entityManager.createNativeQuery(
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						jointApplicantRAAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("Customer Number");

							prefix = "MAC_ADD";

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(jointApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							jointApplicantjson.get(ja).put("MAC_ADD", jointApplicantRAJsom.get(0));
						}

						/////////////////////////// ///////////////////////////////////////////////////////////////
						/////////////////////////////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select app.\"Application Number\",adds.\"Customer Number\",adds.\"Mobile No\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Mobile No\" is not null and app.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							jointApplicantjson.get(ja).put("MAC_TEL", mainApplicantRAJsom.get(0));
							prefix = "MAC_TEL";

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
						///////////////////////////// Main Application Property Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {

							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							jointApplicantjson.get(ja).put("MAC_PROP", mainApplicantRAJsom.get(0));
							prefix = "MAC_ADD";
							if(mainApplicantRAJsom!=null && mainApplicantRAJsom.size()>0) {
								for (int p = 0; p < cols.size(); p++) {
									System.out
											.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}}

							// jointApplicantjson.get(0).remove("Application Number");
							// jointApplicantjson.get(0).remove("Customer Number");

						}

						/////////////////////////// Main Application Property Application End
						///////////////////////////// Email
						///////////////////////////// /////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select \"Email ID\" EMA_ADD from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Email ID\" is not null and \"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");

							mainApplicantRAJsom.get(0).remove("Customer Number");
							jointApplicantjson.get(ja).put("MAC_EMA", mainApplicantRAJsom.get(0));
							prefix = "MAC_EMA";

							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}
						//////////////////////////////////////////////////////////////////////

						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("SME", array);
					json.get(app).set("SMES", result);

				}

				///////////////////////////////////// Company Details END
				///////////////////////////////////// ////////////////////////////////////////////////

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
				// String
				prefix = "";
				if (jointApplicant != null && !jointApplicant.isEmpty()) {
					jointApplicantAll.addAll(jointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(jointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "CP";
						if (ja > 0) {
							prefix = "CP" + ja;
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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

							jointApplicantjson.get(ja).put("CP_CA", jointApplicantRAJsom.get(0));
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
								prefix = "CP_RA";
								if (docid > 0) {
									prefix = "CP_RA" + docid;
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
							JsonNode result = mapper.createObjectNode().set("CP_ID", array);
							jointApplicantjson.get(ja).set("CPDOC", result);
						}

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("CP", array);
					json.get(app).set("CPS", result);

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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			// String re =
			// "[^\\u0009\\u000A\\u000D\\u0020-\\uD7FF\\uE000-\\uFFFD\\u0001\\u0000-\\u0010\\uFFFF]";
			// batch.toString().replaceAll(re, "");
			XmlMapper xmlMapper = new XmlMapper();
			// xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
			// xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_1_1, true);
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<CPS>", "").replace("</CPS>", "");
			xml = xml.replace("<SMES>", "").replace("</SMES>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MPDOC>", "").replace("</MPDOC>", "");
			xml = xml.replace("<CPDOC>", "").replace("</CPDOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`(2) from dual";

			String fileNo = osourceTemplate.queryForObject(sql, String.class);

			//////////////////////////// GENERATE XML FILE and Send Email

			////////// GENERATE XLS LOGIC/////////////////////////////////////////////

			if (fetchapplication != null && fetchapplication.size() > 0) {

				FileOutputStream fileOut = new FileOutputStream(fileNo + ".xlsx");
				workbook.write(fileOut);
				fileOut.close();

				System.out.println("file generated successfully file no"+fileNo);

				/// GENERATE XML AND SEND EMAIL//////////////////////////////
				boolean filepath = stringToDom(createXml, fileNo + ".xml", hour);

				GeneratedKeyHolder holder = new GeneratedKeyHolder();
				osourceTemplate.update(new PreparedStatementCreator() {
					@Override
					public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
						PreparedStatement statement = con.prepareStatement(
								"INSERT INTO hunter_job_nonindividual (emailsend, createon, jobfile, jobdataxml, emailsendon) VALUES (?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP) ",
								Statement.RETURN_GENERATED_KEYS);
						statement.setString(1, String.valueOf(filepath ? 1 : 0));
						statement.setString(2, fileNo);
						statement.setString(3, createXml);
						return statement;
					}
				}, holder);

				long primaryKey = holder.getKey().longValue();

				String sqls = "INSERT INTO `hunter_job_nonindividual_application` (`createon`, `applicationnumber`, `hunter_job_id`) VALUES (CURRENT_TIMESTAMP, ?, ?)";

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

	////////////////////////////////// SEM DATA
	////////////////////////////////// END///////////////////////////////////////////////////////////

	///////////////////////////////////////////////// ADHOC DATA ONLY EXTRACTION
	///////////////////////////////////////////////// ////////////////////////////////////////////////////////
	@GetMapping("/fetchDataPerNeed")
	public List<ObjectNode> fetchDataPerNeed() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
			// hour="AM";
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
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
					+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.VERDICT_DATE between TO_TIMESTAMP ('16-Sep-23 15:59:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('22-Sep-23 08:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in ('APPL05286151','APPL05285058','APPL05285980','APPL05286305','APPL05286304','APPL05285942','APPL05283947','APPL05286942','APPL05287531','APPL05288051','APPL05287001','APPL05277828','APPL05285119','APPL05286980','APPL05287424','APPL05285136','APPL05286992','APPL05286053','APPL05286328','APPL05286079','APPL05285112','APPL05285816','APPL05287479','APPL05286720','APPL05281218','APPL05286599','APPL05287471','APPL05287434','APPL05287343','APPL05280290','APPL05286447','APPL05285255','APPL05287652','APPL05283608','APPL05288254','APPL05287145','APPL05287337','APPL05276882','APPL05286559','APPL05280835','APPL05286217','APPL05287351','APPL05286621','APPL05286837','APPL05286843','APPL05286852','APPL05287674','APPL05287171','APPL05285533','APPL05285341','APPL05286091','APPL05286549','APPL05285912','APPL05286648','APPL05276450','APPL05287325','APPL05288073','APPL05287423','APPL05285327','APPL05287016','APPL05286956','APPL05286845','APPL05287724','APPL05286877','APPL05286894','APPL05287314','APPL05287698','APPL05285383','APPL05286054','APPL05286278','APPL05286037','APPL05286908','APPL05286699','APPL05286463','APPL05284008','APPL05285315','APPL05286342','APPL05285325','APPL05287122','APPL05287731','APPL05288432','APPL05287257','APPL05286603','APPL05274841','APPL05281119','APPL05287483','APPL05284067','APPL05283295','APPL05287923','APPL05287163','APPL05286848','APPL05287953','APPL05287237','APPL05287243','APPL05286694','APPL05287278','APPL05287198','APPL05284872','APPL05283668','APPL05286388','APPL05284753','APPL05286384','APPL05274754','APPL05278595','APPL05286903','APPL05286659','APPL05286545','APPL05286335','APPL05285933','APPL05285222','APPL05286689','APPL05285260','APPL05280420','APPL05287469','APPL05286986','APPL05285952','APPL05286898','APPL05286933','APPL05286432','APPL05287207','APPL05287007','APPL05287147','APPL05282707','APPL05286294','APPL05284923','APPL05284964','APPL05286012','APPL05284980','APPL05285440','APPL05288237','APPL05286899','APPL05286862','APPL05282332','APPL05286197','APPL05284939','APPL05286138','APPL05287750','APPL05286222','APPL05287126','APPL05287334','APPL05286575','APPL05287374','APPL05287028','APPL05286536','APPL05286448','APPL05287273','APPL05288024','APPL05286179','APPL05287210','APPL05287283','APPL05287287','APPL05287654','APPL05277639','APPL05287777','APPL05288065','APPL05286930','APPL05287368','APPL05285234','APPL05287298','APPL05286343','APPL05287148','APPL05287618','APPL05285731','APPL05284896','APPL05286472','APPL05286460','APPL05277784','APPL05285733','APPL05284877','APPL05287993','APPL05287490','APPL05287448','APPL05287659','APPL05287260','APPL05280441','APPL05287319','APPL05286900','APPL05286964','APPL05287977','APPL05286943','APPL05287418','APPL05286555','APPL05286330','APPL05286881','APPL05288238','APPL05287185','APPL05287413','APPL05286810','APPL05277044','APPL05286225','APPL05287230','APPL05287240','APPL05286754','APPL05277928','APPL05287164','APPL05287244','APPL05285825','APPL05285543','APPL05284981','APPL05280471','APPL05284894','APPL05286593','APPL05287515','APPL05287356','APPL05286264','APPL05286257','APPL05283654','APPL05279624','APPL05287439','APPL05287014','APPL05286878','APPL05287011','APPL05286184','APPL05286224','APPL05283733','APPL05284637','APPL05287954','APPL05286585','APPL05286892','APPL05286982','APPL05284947','APPL05287127','APPL05287276','APPL05287626','APPL05286944','APPL05286935','APPL05286936','APPL05280513','APPL05286891','APPL05287157','APPL05287286','APPL05287263','APPL05286591','APPL05287673','APPL05286326','APPL05286324','APPL05283925','APPL05286428','APPL05287938','APPL05285289','APPL05286193','APPL05287331','APPL05286876','APPL05286950','APPL05286652','APPL05287657','APPL05283826','APPL05287329','APPL05286724','APPL05287203','APPL05287328','APPL05288012','APPL05287015','APPL05285991','APPL05277761','APPL05286125','APPL05287432','APPL05287125','APPL05286229','APPL05287307','APPL05284331','APPL05286361','APPL05287718','APPL05287480','APPL05287249','APPL05286940','APPL05285740','APPL05283859','APPL05273628','APPL05286916','APPL05286934','APPL05287139','APPL05286834','APPL05286663','APPL05287420','APPL05286133','APPL05286376','APPL05286323','APPL05286470','APPL05287507','APPL05287309','APPL05285367','APPL05286082','APPL05286620','APPL05286961','APPL05287456','APPL05287363','APPL05287387','APPL05287386','APPL05287026','APPL05287518','APPL05286627','APPL05287470','APPL05286960','APPL05285923','APPL05286154','APPL05286028','APPL05286910','APPL05287708','APPL05287291','APPL05286863','APPL05287767','APPL05288086','APPL05286195','APPL05286215','APPL05286220','APPL05287232','APPL05287248','APPL05286979','APPL05285811','APPL05283420','APPL05285748','APPL05285043','APPL05279720','APPL05285758','APPL05287181','APPL05287179','APPL05287253','APPL05283609','APPL05285558','APPL05286902','APPL05286887','APPL05286089','APPL05286272','APPL05285530','APPL05286757','APPL05287217','APPL05287690','APPL05286105','APPL05286090','APPL05286255','APPL05286068','APPL05286214','APPL05283096','APPL05287136','APPL05286529','APPL05286282','APPL05280216','APPL05286271','APPL05286024','APPL05286823','APPL05287176','APPL05287172','APPL05287339','APPL05287144','APPL05286636','APPL05286655','APPL05287346','APPL05286578','APPL05285818','APPL05287008','APPL05284233','APPL05286938','APPL05285377','APPL05287960','APPL05279960','APPL05286182','APPL05286437','APPL05285757','APPL05286613','APPL05287211','APPL05286638','APPL05283718','APPL05283980','APPL05286246','APPL05286632','APPL05286732','APPL05283401','APPL05288019','APPL05276835','APPL05285147','APPL05287117','APPL05286977','APPL05287214','APPL05286104','APPL05288056','APPL05287204','APPL05287419','APPL05286312','APPL05286286','APPL05286420','APPL05286540','APPL05286445','APPL05285953','APPL05286279','APPL05285517','APPL05284384','APPL05286709','APPL05287349','APPL05286362','APPL05286547','APPL05285723','APPL05286185','APPL05285103','APPL05286912','APPL05286949','APPL05286947','APPL05285987','APPL05286295','APPL05286526','APPL05286700','APPL05287292','APPL05283551','APPL05279325','APPL05285251','APPL05287753','APPL05286121','APPL05286177') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null) ";

			String dbapp = "SELECT applicationnumber from hunter_job_application";

			List<String> dbapplist =null;// osourceTemplate.queryForList(dbapp, String.class);

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
								+ " and  \"First Name\" is not null and \"Application Number\" in ('" + appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
						if(mainApplicantRAJsom!=null && mainApplicantRAJsom.size()>0) {
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
						}

						///////////////////////////// Main Application Residential Application End
						///////////////////////////// ///////////////////////////////////////////////////////////////

						///////////////////////////// Main Application Permanant Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
						if(mainApplicantRAJsom!=null && mainApplicantRAJsom.size()>0 ) {
							for (int p = 0; p < cols.size(); p++) {

								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									System.out.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									row.createCell((short) p).setCellValue(
											mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							mainApplicantjson.get(mainapp).put("MA_PMA", mainApplicantRAJsom.get(0));
						}


						/////////////////////////// Main Application Permanant Application End
						/////////////////////////// ///////////////////////////////////////////////////////////////
						///////////////////////////// Main Application Property Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
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
								"select  app.\"Application Number\",case when adds.\"Occupation Type\" like 'Self Employed' then adds.\"Organization Name\" else adds.\"Employer Name\" end ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
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
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and (adds.\"Organization Name\" is not null or adds.\"Employer Name\" is not null) and app.\"Application Number\" in ('"
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
										"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								if (employeraddressJsom != null && employeraddressJsom.size() > 0) {
									mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
									prefix = prefix + "_EMP";
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(
												employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
										if (employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
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
								if (employerTelephone != null && employerTelephone.size() > 0) {
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")));
										if (employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
									}
									mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
								}
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);

			String fileNo = "ADHOC22SEP23";

			//////////////////////////// GENERATE XML FILE and Send Email

			////////// GENERATE XLS LOGIC/////////////////////////////////////////////

			if (fetchapplication != null && fetchapplication.size() > 0) {

				FileOutputStream fileOut = new FileOutputStream(fileNo + ".xlsx");
				workbook.write(fileOut);
				fileOut.close();

				System.out.println("file generated successfully");

				/// GENERATE XML AND SEND EMAIL//////////////////////////////
				// boolean filepath = stringToDom(createXml, fileNo + ".xml", hour);

				DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
				DocumentBuilder builder = factory.newDocumentBuilder();
				Document doc = builder.parse(new InputSource(new StringReader(createXml)));

				// Use a Transformer for output
				TransformerFactory tFactory = TransformerFactory.newInstance();
				Transformer transformer = tFactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");

				DOMSource source = new DOMSource(doc);
				StreamResult results = new StreamResult(new File(fileNo));
				transformer.transform(source, results);

				System.out.println(createXml);
				JsonNode resultjson = mapper.createObjectNode().set("SUBMISSION", array);
				System.out.println(resultjson.asText());

			}

			//////////////////////////// End/////////////////////
			/////////////////////////////////////////

		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();

		}

		return json;

	}

	////////////////////////////////////////// AS PER
	////////////////////////////////////////// NEEDED///////////////////////////////////////////////////////////////

	/////////////////////////////////////////////////////////////////////////////////// DAILY
	/////////////////////////////////////////////////////////////////////////////////// HUNTER
	/////////////////////////////////////////////////////////////////////////////////// DATA
	/////////////////////////////////////////////////////////////////////////////////// START

	/////////////////////////////////////////////////////////////////////////////////// START
	@GetMapping("/fetchData")
	public List<ObjectNode> fetchData() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
			// hour="AM";
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;
			if (hour.contains("am")) {
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
						+ mailconfig.getTodate()
						+ " 08:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null) ";

			} else if (hour.contains("pm")) {
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
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.VERDICT_DATE between "
						+ " TO_TIMESTAMP ('" + mailconfig.getTodate() + " 09:00:00', 'DD-Mon-RR HH24:MI:SS') and "
						+ " TO_TIMESTAMP ('" + mailconfig.getTodate() + " 15:59:59', 'DD-Mon-RR HH24:MI:SS') "
						+ " and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null)";
			}

			String dbapp = "SELECT distinct applicationnumber from hunter_job_application";

			List<String> dbapplist = osourceTemplate.queryForList(dbapp, String.class);

			/*
			 * if (dbapplist != null && dbapplist.size() > 0) {
			 *
			 * List<List<String>> partitions = Lists.partition(dbapplist, 999);
			 * System.out.println(partitions.size());
			 *
			 * for (int p = 0; p < partitions.size(); p++) { querysubmission +=
			 * " and app.\"Application Number\" not in (" +
			 * partitions.get(p).stream().collect(Collectors.joining("','", "'", "'")) +
			 * ")";
			 *
			 * } q = entityManager.createNativeQuery(querysubmission, Tuple.class);
			 *
			 * } else { q = entityManager.createNativeQuery(querysubmission, Tuple.class); }
			 */
			q = entityManager.createNativeQuery(querysubmission, Tuple.class);
			System.out.println("before fetch query");
			List<Tuple> fetchapplicationiterate = new ArrayList<>();
			fetchapplicationiterate = q.getResultList();

			if (fetchapplicationiterate != null && fetchapplicationiterate.size() > 0) {

				for (int i = 0; i < fetchapplicationiterate.size(); i++) {

					List<TupleElement<?>> colsd = fetchapplicationiterate.get(i).getElements();

					ObjectNode one = mapper.createObjectNode();

					for (TupleElement col : colsd) {
						if (col != null && col.getAlias() != null
								&& fetchapplicationiterate.get(i).get(col.getAlias()) != null) {

							if (col.getAlias().equalsIgnoreCase("Application Number")) {
								if (dbapplist != null && dbapplist.size() > 0
										&& !dbapplist.contains(fetchapplicationiterate.get(i).get(col.getAlias()).toString())) {

									fetchapplication.add(fetchapplicationiterate.get(i));
								}

							}

						}
					}

				}
			}

			System.out.println("before fetch query");
			json = _toJson(fetchapplication);

			int rowCount = 1;
			for (int app = 0; app < json.size(); app++) {

				String appNo = json.get(app).get("Application Number").asText();
				appList.add(appNo);
				String custNo = json.get(app).get("Customer Number").asText();
				System.out.println(appNo + "                    " + custNo);
				System.out.println(appNo + "currently running index " + app + "legth of " + json.size());

				if (dbapplist != null && dbapplist.size() > 0 && !dbapplist.contains(appNo)) {

					Row row = huntersheet.createRow(rowCount++);
					for (int p = 0; p < cols.size(); p++) {
						if (json.get(app).get(cols.get(p)) != null) {
							row.createCell((short) p).setCellValue(json.get(app).get(cols.get(p)).asText());
						}
					}

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
									+ " and  \"First Name\" is not null and \"Application Number\" in ('" + appNo
									+ "')",
							Tuple.class);
					@SuppressWarnings("unchecked")
					List<Tuple> mainApplicant = q.getResultList();

					System.out.println(appNo + " data size " + mainApplicant);

					if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
						mainApplicantAll.addAll(mainApplicant);

						List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
						for (int mainapp = 0; mainapp < mainApplicantjson.size(); mainapp++) {
							String prefix = "MA";
							if (mainapp > 0) {
								prefix = "MA" + mainapp;
							}
							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantjson.get(mainapp).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantjson.get(mainapp)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
							mainApplicantjson.get(mainapp).remove("Application Number");
							/////////////////////////////// Main Application Residential Application
							/////////////////////////////// ////////////////////////////////////////
							q = entityManager.createNativeQuery(
									"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
							if(mainApplicantRAJsom!=null && mainApplicantRAJsom.size()>0) {
								for (int p = 0; p < cols.size(); p++) {
									System.out
											.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
									if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
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
									"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
							if(mainApplicantRAJsom!=null && mainApplicantRAJsom.size()>0) {
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
								mainApplicantjson.get(mainapp).put("MA_PMA", mainApplicantRAJsom.get(0));
							}

							/////////////////////////// Main Application Permanant Application End
							/////////////////////////// ///////////////////////////////////////////////////////////////
							///////////////////////////// Main Application Property Application
							///////////////////////////// ///////////////////////////////////////////////////////////////
							q = entityManager.createNativeQuery(
									"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
											+ appNo + "')",
									Tuple.class);

							mainApplicantRA = q.getResultList();

							if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
								mainApplicantPAAll.addAll(mainApplicantRA);
								mainApplicantRAJsom = _toJson(mainApplicantRA);
								prefix = "MA_PRE";
								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
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
									System.out.println(
											mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
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
									System.out.println(
											mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
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
									System.out.println(
											mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
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
										System.out.println(mainApplicantRAJsom.get(docid)
												.get(cols.get(p).replace(prefix + "_", "")));
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
									"select  app.\"Application Number\",case when adds.\"Occupation Type\" like 'Self Employed' then adds.\"Organization Name\" else adds.\"Employer Name\" end ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
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
											+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and (adds.\"Organization Name\" is not null or adds.\"Employer Name\" is not null) and app.\"Application Number\" in ('"
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
										System.out.println(mainApplicantRAJsom.get(emp)
												.get(cols.get(p).replace(prefix + "_", "")));
										if (mainApplicantRAJsom.get(emp)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(emp)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
									}

									// Employer Address
									q = entityManager.createNativeQuery(
											"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
													+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
													+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
													+ custNo + "')",
											Tuple.class);

									@SuppressWarnings("unchecked")
									List<Tuple> employeraddress = q.getResultList();

									List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
									if (employeraddressJsom != null && employeraddressJsom.size() > 0) {
										mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
										prefix = prefix + "_EMP";
										for (int p = 0; p < cols.size(); p++) {
											System.out.println(employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")));
											if (employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")) != null) {
												row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
														.get(cols.get(p).replace(prefix + "_", "")).asText());
											}
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
									if (employerTelephone != null && employerTelephone.size() > 0) {
										for (int p = 0; p < cols.size(); p++) {
											System.out.println(employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")));
											if (employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")) != null) {
												row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
														.get(cols.get(p).replace(prefix + "_", "")).asText());
											}
										}
										mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
									}
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
								System.out
										.println(jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
								if (jointApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(jointApplicantjson.get(ja)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
							System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
							custNo = jointApplicantjson.get(ja).get("Customer Number").asText();
							if (jointApplicantjson.get(ja).has("Neo CIF ID")) {
								neoCIFID = jointApplicantjson.get(ja).get("Neo CIF ID").asText();
							}
							jointApplicantjson.get(ja).remove("Neo CIF ID");
							q = entityManager.createNativeQuery(
									"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
									if (jointApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
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
							if (brokerAddress != null && brokerAddress.size() > 0) {
								List<ObjectNode> brokerAddressJsom = _toJson(brokerAddress);
								brokerAddressJsom.get(0).remove("Referral Code");
								brokerjson.get(br).put("BR_ADD", brokerAddressJsom.get(0));

								for (int p = 0; p < cols.size(); p++) {
									System.out.println(
											brokerAddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
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
									+ appNo + "') and NAME is not null and rownum<4",
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
								System.out.println(
										referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")));
								if (referenceApplicantjson.get(ja).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}
							referenceApplicantjson.get(ja).remove("APPLICATION_NUMBER");
							System.out.println("jointApplicantjson index- " + ja + " --  " + custNo);
							q = entityManager.createNativeQuery(
									"select APPLICATION_NUMBER,COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", '') \"ADD\", \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in  ('"
											+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
									if (referenceApplicantjson.get(ja)
											.get(cols.get(p).replace(prefix + "_", "")) != null) {
										row.createCell((short) p).setCellValue(referenceApplicantjson.get(ja)
												.get(cols.get(p).replace(prefix + "_", "")).asText());
									}
								}
							}

							q = entityManager.createNativeQuery(
									"select APPLICATION_NUMBER,TO_CHAR(\"Mobile Number\") TEL_NO from NEO_CAS_LMS_SIT1_SH.REFERENCE_DETAILS adds where adds.APPLICATION_NUMBER in ('"
											+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`(1) from dual";

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
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		return json;

	}

	@GetMapping("/fetchDataold")
	public List<ObjectNode> fetchDataold() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
			// hour="AM";
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
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
						+ mailconfig.getTodate()
						+ " 08:59:59', 'DD-Mon-RR HH24:MI:SS') and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null) ";

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
						+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null and app.VERDICT_DATE between "
						+ " TO_TIMESTAMP ('" + mailconfig.getTodate() + " 09:00:00', 'DD-Mon-RR HH24:MI:SS') and "
						+ " TO_TIMESTAMP ('" + mailconfig.getTodate() + " 15:59:59', 'DD-Mon-RR HH24:MI:SS') "
						+ " and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null)";
			}

			String dbapp = "SELECT distinct applicationnumber from hunter_job_application";

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

			System.out.println("before fetch query");
			fetchapplication = q.getResultList();
			System.out.println("before fetch query");
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
				System.out.println(appNo + "currently running index " + app + "legth of " + json.size());
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
								+ " and  \"First Name\" is not null and \"Application Number\" in ('" + appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
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
								"select  app.\"Application Number\",case when adds.\"Occupation Type\" like 'Self Employed' then adds.\"Organization Name\" else adds.\"Employer Name\" end ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
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
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and (adds.\"Organization Name\" is not null or adds.\"Employer Name\" is not null) and app.\"Application Number\" in ('"
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
										"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								if (employeraddressJsom != null && employeraddressJsom.size() > 0) {
									mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
									prefix = prefix + "_EMP";
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(
												employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
										if (employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
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
								if (employerTelephone != null && employerTelephone.size() > 0) {
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")));
										if (employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
									}
									mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
								}
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`(1) from dual";

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
								"select  SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						List<Tuple> mainApplicantRA = q.getResultList();
						List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
						mainApplicantjson.get(j).put("MA_CA", mainApplicantRAJsom.get(0));

						q = entityManager.createNativeQuery(
								"select  SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
								"select  SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
//		StreamResult result = new StreamResult(new File(fileNo));

		StringWriter writer = new StringWriter();
		StreamResult result= new StreamResult(writer);
		transformer.transform(source, result);

		String xmlOutput = writer.toString();
		xmlOutput = xmlOutput.replace(" standalone=\"no\"", "");

		FileWriter fileWriter = new FileWriter(fileNo);
		fileWriter.write(xmlOutput);
		fileWriter.close();

		System.out.println("XML file created successfully without standalone attribute!");


		try {

			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%m-%Y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%m-%Y') fromdate FROM email_config",
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
			// Thread emailThread = new Thread(sendemail);
			// emailThread.start();

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
//		StreamResult result = new StreamResult(new File(fileNo));

		StringWriter writer = new StringWriter();
		StreamResult result= new StreamResult(writer);
		transformer.transform(source, result);

		String xmlOutput = writer.toString();
		xmlOutput = xmlOutput.replace(" standalone=\"no\"", "");

		FileWriter fileWriter = new FileWriter(fileNo);
		fileWriter.write(xmlOutput);
		fileWriter.close();
		System.out.print("File created without standalone atribute");

		try {

			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%m-%Y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%m-%Y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));

			// sendEmail(fileNo, hour);

			String toemail = "vijay.uniyal@shubham.co";
			String subject = "";
			String bodypart = "";
			if (hour.contains("am")) {
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

	////////////////////////////////////////////////////////// fetch History
	////////////////////////////////////////////////////////// ////////////////////////////////
	@GetMapping("/fetchSMEDatahistory")
	public List<ObjectNode> fetchSMEDatahistory() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MP_PAN",
				"MP_FST_NME", "MP_MID_NME", "MP_LST_NME", "MP_DOB", "MP_AGE", "MP_GNDR", "MP_NAT_CDE", "MP_PA_ADD",
				"MP_PA_CTY", "MP_PA_STE", "MP_PA_CTRY", "MP_PA_PIN", "MP_RA_ADD", "MP_RA_CTY", "MP_RA_STE",
				"MP_RA_CTRY", "MP_RA_PIN", "MP_PRE_ADD", "MP_PRE_CTY", "MP_PRE_STE", "MP_PRE_CTRY", "MP_PRE_PIN",
				"MP_HT_TEL_NO", "MP_M_TEL_NO", "SME_ORG_NME", "SME_EMP_IND", "SME_CONSTIT", "SME_TAN_NO", "SME_DAT_INC",
				"SME_CREG_NO", "SME_ORG_STDAT", "SME_SAL_TXN", "SME_TURNOV", "SME_EMP_NO", "SME_GST_NO", "SME_STATUS",
				"MAC_ADD_ADD", "MAC_ADD_CTY", "MAC_ADD_STE", "MAC_ADD_CTRY", "MAC_ADD_PIN", "MAC_ADD_STATUS",
				"MAC_TEL3_TEL_NO", "MAC_TEL3_EXT_NO", "MAC_TEL3_STATUS", "MAC_TEL1_TEL_NO", "MAC_TEL1_EXT_NO",
				"MAC_TEL1_STATUS", "MAC_TEL2_TEL_NO", "MAC_TEL2_EXT_NO", "MAC_TEL2_STATUS", "MAC_EMA2_ADD",
				"MAC_EMA2_CO_ADD", "MAC_EMA2_DO_NAM", "MAC_EMA2_STATUS", "MAC_EMA1_ADD", "MAC_EMA1_CO_ADD",
				"MAC_EMA1_DO_NAM", "MAC_EMA1_STATUS", "MP_EMA_ADD", "MP1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
				"MP_DOC_TYP", "MP_DOC_NO", "MP1_DOC_TYP", "MP1_DOC_NO", "MP2_DOC_TYP", "MP2_DOC_NO", "MP3_DOC_TYP",
				"MP3_DOC_NO", "MP4_DOC_TYP", "MP4_DOC_NO", "MP5_DOC_TYP", "MP5_DOC_NO", "MP6_DOC_TYP", "MP6_DOC_NO",
				"MP7_DOC_TYP", "MP7_DOC_NO", "MP8_DOC_TYP", "MP8_DOC_NO", "MP9_DOC_TYP", "MP9_DOC_NO", "MP_ORG_NME",
				"MP_EMP_IND", "MP1_ORG_NME", "MP1_EMP_IND", "MP_EMP_ADD", "MP_EMP_CTY", "MP_EMP_STE", "MP_EMP_CTRY",
				"MP_EMP_PIN", "MP_EMP_TEL", "MP1_EMP_ADD", "MP1_EMP_CTY", "MP1_EMP_STE", "MP1_EMP_CTRY", "MP1_EMP_PIN",
				"MP1_EMP_TEL", "CP_PAN", "CP_FST_NME", "CP_MID_NME", "CP_LST_NME", "CP_DOB", "CP_AGE", "CP_GNDR",
				"CP1_PAN", "CP1_FST_NME", "CP1_MID_NME", "CP1_LST_NME", "CP1_DOB_1", "CP1_AGE_1", "CP1_GNDR_1",
				"CP2_PAN", "CP2_FST_NME", "CP2_MID_NME", "CP2_LST_NME", "CP2_DOB", "CP2_AGE", "CP2_GNDR", " CP_RA_ADD",
				"CP_RA_CTY", "CP_RA_STE", "CP_RA_CTRY", "CP_RA_PIN", "CP1_RA_ADD", "CP1_RA_CTY", "CP1_RA_STE",
				"CP1_RA_CTRY", "CP1_RA_PIN", "CP2_RA_ADD", "CP2_RA_CTY", "CP2_RA_STE", "CP2_RA_CTRY", "CP2_RA_PIN",
				"CP_RA_DOC_TYP_1", "CP_RA_DOC_NO_1", "CP_RA_DOC_TYP_2", "CP_RA_DOC_NO_2", "CP_RA_DOC_TYP_3",
				"CP_RA_DOC_NO_3", "CP_RA_DOC_TYP_4", "CP_RA_DOC_NO_4", "CP_RA_DOC_TYP_5", "CP_RA_DOC_NO_5",
				"CP_RA_DOC_TYP_6", "CP_RA_DOC_NO_6", "CP_RA_DOC_TYP_7", "CP_RA_DOC_NO_7", "CP_RA_DOC_TYP_8",
				"CP_RA_DOC_NO_8", "CP_RA_DOC_TYP_9", "CP_RA_DOC_NO_9", "CP_RA_DOC_TYP_10", "CP_RA_DOC_NO_10",
				"CP1_RA_DOC_TYP_1", "CP1_RA_DOC_NO_1", "CP1_RA_DOC_TYP_2", "CP1_RA_DOC_NO_2", "CP1_RA_DOC_TYP_3",
				"CP1_RA_DOC_NO_3", "CP1_RA_DOC_TYP_4", "CP1_RA_DOC_NO_4", "CP1_RA_DOC_TYP_5", "CP1_RA_DOC_NO_5",
				"CP1_RA_DOC_TYP_6", "CP1_RA_DOC_NO_6", "CP1_RA_DOC_TYP_7", "CP1_RA_DOC_NO_7", "CP1_RA_DOC_TYP_8",
				"CP1_RA_DOC_NO_8", "CP1_RA_DOC_TYP_9", "CP1_RA_DOC_NO_9", "CP1_RA_DOC_TYP_10", "CP1_RA_DOC_NO_10",
				"CP2_RA_DOC_TYP_1", "CP2_RA_DOC_NO_1", "CP2_RA_DOC_TYP_2", "CP2_RA_DOC_NO_2", "CP2_RA_DOC_TYP_3",
				"CP2_RA_DOC_NO_3", "CP2_RA_DOC_TYP_4", "CP2_RA_DOC_NO_4", "CP2_RA_DOC_TYP_5", "CP2_RA_DOC_NO_5",
				"CP2_RA_DOC_TYP_6", "CP2_RA_DOC_NO_6", "CP2_RA_DOC_TYP_7", "CP2_RA_DOC_NO_7", "CP2_RA_DOC_TYP_8",
				"CP2_RA_DOC_NO_8", "CP2_RA_DOC_TYP_9", "CP2_RA_DOC_NO_9", "CP2_RA_DOC_TYP_10", "CP2_RA_DOC_NO_10",
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
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
					new BeanPropertyRowMapper(QrtzEmailConfig.class));
			Query q = null;

			querysubmission = "select app.\"Sanction Loan Amount\" APP_VAL,app.\"Sanction Tenure\" TERM,app.\"Neo CIF ID\",app.\"Customer Number\" ,app.\"Application Number\",'UNKNOWN' CLASSIFICATION,case when br.\"Branch Region\" is not null then SUBSTR(br.\"Branch Region\",1,1) else 'N' end||'_'||app.\"Application Number\"||'_'||case when app.\"Product Type Code\" like 'HL' then 'HOU' else 'NHOU' end  IDENTIFIER,'SME_ACC' PRODUCT, to_char(sysdate,'YYYY-MM-DD')  \"DATE\" ,to_char(app.\"Sanction Date\",'YYYY-MM-DD') APP_DTE ,CASE\r\n"
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
					+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null "
					+ " and app.VERDICT_DATE between TO_TIMESTAMP ('25-Jan-23 09:00:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('25-Jan-23 15:59:59', 'DD-Mon-RR HH24:MI:SS') "
					+ "and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Applicant Type\"='Primary Applicant')";// and
			// app.\"Application
			// Number\"
			// in
			// ('APPL05199249','APPL05199693','APPL05198249','APPL05197802','APPL05197543','APPL05197551','APPL05192314','APPL05192622','APPL05192239','APPL05192240')

			System.out.println(querysubmission);
			String dbapp = "SELECT applicationnumber from hunter_job_nonindividual_application";

			List<String> dbapplist = osourceTemplate.queryForList(dbapp, String.class);
			dbapplist = null;
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
						"select \"Application Number\", case when \"Identification Type\" like 'PAN' then \"Identification Number\" end PAN,\"Institution Name\" FST_NME, null MID_NME,\"Institution Name\" LST_NME,\r\n"
								+ "                                to_char(\"INCORPORATION_DATE\",'YYYY-MM-DD') DOB from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Applicant Type\"='Primary Applicant' and \"Identification Type\" like 'PAN'\r\n"
								+ "								 and  \"Institution Name\" is not null and \"Application Number\" in ('"
								+ appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
					mainApplicantAll.addAll(mainApplicant);

					List<ObjectNode> mainApplicantjson = _toJson(mainApplicant);
					for (int mainapp = 0; mainapp < mainApplicantjson.size(); mainapp++) {
						String prefix = "MP";
						if (mainapp > 0) {
							prefix = "MP" + mainapp;
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
						if (mainApplicantRAJsom != null && mainApplicantRAJsom.size() > 0) {
							prefix = "MP_RA";
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
							mainApplicantjson.get(mainapp).put("MP_CA", mainApplicantRAJsom.get(0));
						}

						///////////////////////////// Main Application Residential Application End
						///////////////////////////// ///////////////////////////////////////////////////////////////

						///////////////////////////// Main Application Permanant Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Address 1\" is not null  and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantPAAll.addAll(mainApplicantRA);
						}

						mainApplicantRAJsom = _toJson(mainApplicantRA);
						if (mainApplicantRAJsom != null && mainApplicantRAJsom.size() > 0) {
							prefix = "MP_PA";
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
							mainApplicantjson.get(mainapp).put("MP_PMA", mainApplicantRAJsom.get(0));
						}

						/////////////////////////// Main Application Permanant Application End
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
							mainApplicantjson.get(mainapp).put("MP_HT", mainApplicantRAJsom.get(0));
							prefix = "MP_HT";
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
							mainApplicantjson.get(mainapp).put("MP_MT", mainApplicantRAJsom.get(0));
							prefix = "MP_M";
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
							mainApplicantjson.get(mainapp).put("MP_EMA", mainApplicantRAJsom.get(0));
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

							mainApplicantjson.get(mainapp).put("MP_BNK", mainApplicantRAJsom.get(0));

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
								prefix = "MP";
								if (docid > 0) {
									prefix = "MP" + docid;
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
							JsonNode result = mapper.createObjectNode().set("MP_ID", array);
							mainApplicantjson.get(mainapp).set("MPDOC", result);
						}

						///////////////////// DOCUMENT ID END

					}
					json.get(app).put("MP", mainApplicantjson.get(0));
				}

				// Main Application Data Population End

				///////////////////////////////////// Company Details
				///////////////////////////////////// ////////////////////////////////////////////////

				q = entityManager.createNativeQuery(
						"select distinct \"Customer Number\",\"Institution Name\" ORG_NME,\"REGISTRATION_NUMBER\" TAN_NO from NEO_CAS_LMS_SIT1_SH.\"NON_INDIVIDUAL_CUSTOMER\" where \"Application Number\"='"
								+ appNo + "' and \"Applicant Type\"='Primary Applicant'",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> empjointApplicant = q.getResultList();
				String prefix;
				if (empjointApplicant != null && !empjointApplicant.isEmpty()) {
					jointApplicantAll.addAll(empjointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(empjointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "SME";
						if (ja > 0) {
							prefix = "SME" + ja;
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
						jointApplicantjson.get(ja).remove("Customer Number");
						q = entityManager.createNativeQuery(
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> jointApplicantRA = q.getResultList();
						jointApplicantRAAll.addAll(jointApplicantRA);
						List<ObjectNode> jointApplicantRAJsom = _toJson(jointApplicantRA);
						if (!jointApplicantRAJsom.isEmpty()) {
							jointApplicantRAJsom.get(0).remove("Customer Number");

							prefix = "MAC_ADD";

							for (int p = 0; p < cols.size(); p++) {
								System.out.println(
										jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (jointApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(jointApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							jointApplicantjson.get(ja).put("MAC_ADD", jointApplicantRAJsom.get(0));
						}

						/////////////////////////// ///////////////////////////////////////////////////////////////
						/////////////////////////////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select app.\"Application Number\",adds.\"Customer Number\",adds.\"Mobile No\" TEL_NO from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where app.\"Application Number\" in ('"
										+ appNo
										+ "') and adds.\"Mobile No\" is not null and app.\"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						@SuppressWarnings("unchecked")
						List<Tuple> mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							jointApplicantjson.get(ja).put("MAC_TEL", mainApplicantRAJsom.get(0));
							prefix = "MAC_TEL";
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
						///////////////////////////// Main Application Property Application
						///////////////////////////// ///////////////////////////////////////////////////////////////
						q = entityManager.createNativeQuery(
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
										+ appNo + "')",
								Tuple.class);

						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {

							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							jointApplicantjson.get(ja).put("MAC_PROP", mainApplicantRAJsom.get(0));
							prefix = "MAC_ADD";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

							// jointApplicantjson.get(0).remove("Application Number");
							// jointApplicantjson.get(0).remove("Customer Number");

						}

						/////////////////////////// Main Application Property Application End
						///////////////////////////// Email
						///////////////////////////// /////////////////////////////////////////////////

						q = entityManager.createNativeQuery(
								"select \"Email ID\" EMA_ADD from NEO_CAS_LMS_SIT1_SH.\"Communication Details\" where \"Email ID\" is not null and \"Customer Number\" in ('"
										+ custNo + "')",
								Tuple.class);
						mainApplicantRA = q.getResultList();

						if (mainApplicantRA != null && mainApplicantRA.size() > 0) {
							mainApplicantMobileAll.addAll(mainApplicantRA);
							List<ObjectNode> mainApplicantRAJsom = _toJson(mainApplicantRA);
							mainApplicantRAJsom.get(0).remove("Application Number");
							mainApplicantRAJsom.get(0).remove("Customer Number");
							jointApplicantjson.get(ja).put("MAC_EMA", mainApplicantRAJsom.get(0));
							prefix = "MAC_EMA";
							for (int p = 0; p < cols.size(); p++) {
								System.out
										.println(mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
								if (mainApplicantRAJsom.get(0).get(cols.get(p).replace(prefix + "_", "")) != null) {
									row.createCell((short) p).setCellValue(mainApplicantRAJsom.get(0)
											.get(cols.get(p).replace(prefix + "_", "")).asText());
								}
							}

						}
						//////////////////////////////////////////////////////////////////////

						///////////////// APPLICANT DOCUMENT ID
						///////////////// //////////////////////////////////////////////////////////////

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("SME", array);
					json.get(app).set("SMES", result);

				}

				///////////////////////////////////// Company Details END
				///////////////////////////////////// ////////////////////////////////////////////////

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
				// String
				prefix = "";
				if (jointApplicant != null && !jointApplicant.isEmpty()) {
					jointApplicantAll.addAll(jointApplicant);

					List<ObjectNode> jointApplicantjson = _toJson(jointApplicant);

					for (int ja = 0; ja < jointApplicantjson.size(); ja++) {
						prefix = "CP";
						if (ja > 0) {
							prefix = "CP" + ja;
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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

							jointApplicantjson.get(ja).put("CP_CA", jointApplicantRAJsom.get(0));
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
								prefix = "CP_RA";
								if (docid > 0) {
									prefix = "CP_RA" + docid;
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
							JsonNode result = mapper.createObjectNode().set("CP_ID", array);
							jointApplicantjson.get(ja).set("CPDOC", result);
						}

						///////////////////// DOCUMENT ID END

						jointApplicantjson.get(ja).remove("Customer Number");
						jointApplicantjson.get(ja).remove("Application Number");

					}
					ArrayNode array = mapper.valueToTree(jointApplicantjson);
					JsonNode result = mapper.createObjectNode().set("CP", array);
					json.get(app).set("CPS", result);

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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			// String re =
			// "[^\\u0009\\u000A\\u000D\\u0020-\\uD7FF\\uE000-\\uFFFD\\u0001\\u0000-\\u0010\\uFFFF]";
			// batch.toString().replaceAll(re, "");
			XmlMapper xmlMapper = new XmlMapper();
			// xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_DECLARATION, true);
			// xmlMapper.configure(ToXmlGenerator.Feature.WRITE_XML_1_1, true);
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<CPS>", "").replace("</CPS>", "");
			xml = xml.replace("<SMES>", "").replace("</SMES>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MPDOC>", "").replace("</MPDOC>", "");
			xml = xml.replace("<CPDOC>", "").replace("</CPDOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`(2) from dual";

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
								"INSERT INTO hunter_job_nonindividual (emailsend, createon, jobfile, jobdataxml, emailsendon) VALUES (?, CURRENT_TIMESTAMP, ?, ?, CURRENT_TIMESTAMP) ",
								Statement.RETURN_GENERATED_KEYS);
						statement.setString(1, String.valueOf(filepath ? 1 : 0));
						statement.setString(2, fileNo);
						statement.setString(3, createXml);
						return statement;
					}
				}, holder);

				long primaryKey = holder.getKey().longValue();

				String sqls = "INSERT INTO `hunter_job_nonindividual_application` (`createon`, `applicationnumber`, `hunter_job_id`) VALUES (CURRENT_TIMESTAMP, ?, ?)";

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

	@GetMapping("/fetchDatahistory")
	public List<ObjectNode> fetchDatahistory() {

		String[] headerArray = new String[] { "IDENTIFIER", "PRODUCT", "CLASSIFICATION", "DATE", "APP_DTE", "LN_PRP",
				"TERM", "APP_VAL", "ASS_ORIG_VAL", "ASS_VAL", "CLNT_FLG", "PRI_SCR", "BRNCH_RGN", "MA_PAN",
				"MA_FST_NME", "MA_MID_NME",
				"MA_LST_NME"/*
							 * , "MA_DOB", "MA_AGE", "MA_GNDR", "MA_NAT_CDE", "MA_PA_ADD", "MA_PA_CTY",
							 * "MA_PA_STE", "MA_PA_CTRY", "MA_PA_PIN", "MA_RA_ADD", "MA_RA_CTY",
							 * "MA_RA_STE", "MA_RA_CTRY", "MA_RA_PIN", "MA_PRE_ADD", "MA_PRE_CTY",
							 * "MA_PRE_STE", "MA_PRE_CTRY", "MA_PRE_PIN", "MA_HT_TEL_NO", "MA_M_TEL_NO",
							 * "MA_EMA_ADD", "MA1_EMA_ADD", "BNK_NM", "ACC_NO", "BRNCH", "MICR",
							 * "MA_DOC_TYP", "MA_DOC_NO", "MA1_DOC_TYP", "MA1_DOC_NO", "MA2_DOC_TYP",
							 * "MA2_DOC_NO", "MA3_DOC_TYP", "MA3_DOC_NO", "MA4_DOC_TYP", "MA4_DOC_NO",
							 * "MA5_DOC_TYP", "MA5_DOC_NO", "MA6_DOC_TYP", "MA6_DOC_NO", "MA7_DOC_TYP",
							 * "MA7_DOC_NO", "MA8_DOC_TYP", "MA8_DOC_NO", "MA9_DOC_TYP", "MA9_DOC_NO",
							 * "MA_ORG_NME", "MA_EMP_IND", "MA1_ORG_NME", "MA1_EMP_IND", "MA_EMP_ADD",
							 * "MA_EMP_CTY", "MA_EMP_STE", "MA_EMP_CTRY", "MA_EMP_PIN", "MA_EMP_TEL",
							 * "MA1_EMP_ADD", "MA1_EMP_CTY", "MA1_EMP_STE", "MA1_EMP_CTRY", "MA1_EMP_PIN",
							 * "MA1_EMP_TEL", "JA_PAN", "JA_FST_NME", "JA_MID_NME", "JA_LST_NME", "JA_DOB",
							 * "JA_AGE", "JA_GNDR", "JA1_PAN", "JA1_FST_NME", "JA1_MID_NME", "JA1_LST_NME",
							 * "JA1_DOB_1", "JA1_AGE_1", "JA1_GNDR_1", "JA2_PAN", "JA2_FST_NME",
							 * "JA2_MID_NME", "JA2_LST_NME", "JA2_DOB", "JA2_AGE", "JA2_GNDR", " JA_RA_ADD",
							 * "JA_RA_CTY", "JA_RA_STE", "JA_RA_CTRY", "JA_RA_PIN", "JA1_RA_ADD",
							 * "JA1_RA_CTY", "JA1_RA_STE", "JA1_RA_CTRY", "JA1_RA_PIN", "JA2_RA_ADD",
							 * "JA2_RA_CTY", "JA2_RA_STE", "JA2_RA_CTRY", "JA2_RA_PIN", "JA_RA_DOC_TYP_1",
							 * "JA_RA_DOC_NO_1", "JA_RA_DOC_TYP_2", "JA_RA_DOC_NO_2", "JA_RA_DOC_TYP_3",
							 * "JA_RA_DOC_NO_3", "JA_RA_DOC_TYP_4", "JA_RA_DOC_NO_4", "JA_RA_DOC_TYP_5",
							 * "JA_RA_DOC_NO_5", "JA_RA_DOC_TYP_6", "JA_RA_DOC_NO_6", "JA_RA_DOC_TYP_7",
							 * "JA_RA_DOC_NO_7", "JA_RA_DOC_TYP_8", "JA_RA_DOC_NO_8", "JA_RA_DOC_TYP_9",
							 * "JA_RA_DOC_NO_9", "JA_RA_DOC_TYP_10", "JA_RA_DOC_NO_10", "JA1_RA_DOC_TYP_1",
							 * "JA1_RA_DOC_NO_1", "JA1_RA_DOC_TYP_2", "JA1_RA_DOC_NO_2", "JA1_RA_DOC_TYP_3",
							 * "JA1_RA_DOC_NO_3", "JA1_RA_DOC_TYP_4", "JA1_RA_DOC_NO_4", "JA1_RA_DOC_TYP_5",
							 * "JA1_RA_DOC_NO_5", "JA1_RA_DOC_TYP_6", "JA1_RA_DOC_NO_6", "JA1_RA_DOC_TYP_7",
							 * "JA1_RA_DOC_NO_7", "JA1_RA_DOC_TYP_8", "JA1_RA_DOC_NO_8", "JA1_RA_DOC_TYP_9",
							 * "JA1_RA_DOC_NO_9", "JA1_RA_DOC_TYP_10", "JA1_RA_DOC_NO_10",
							 * "JA2_RA_DOC_TYP_1", "JA2_RA_DOC_NO_1", "JA2_RA_DOC_TYP_2", "JA2_RA_DOC_NO_2",
							 * "JA2_RA_DOC_TYP_3", "JA2_RA_DOC_NO_3", "JA2_RA_DOC_TYP_4", "JA2_RA_DOC_NO_4",
							 * "JA2_RA_DOC_TYP_5", "JA2_RA_DOC_NO_5", "JA2_RA_DOC_TYP_6", "JA2_RA_DOC_NO_6",
							 * "JA2_RA_DOC_TYP_7", "JA2_RA_DOC_NO_7", "JA2_RA_DOC_TYP_8", "JA2_RA_DOC_NO_8",
							 * "JA2_RA_DOC_TYP_9", "JA2_RA_DOC_NO_9", "JA2_RA_DOC_TYP_10",
							 * "JA2_RA_DOC_NO_10", "RF_FST_NME", "RF_LST_NME", "RF1_FST_NME", "RF1_LST_NME",
							 * "RF2_FST_NME", "RF2_LST_NME", "RF_ADD", "RF_CTY", "RF_STE", "RF_CTRY",
							 * "RF_PIN", "RF1_ADD", "RF1_CTY", "RF1_STE", "RF1_CTRY", "RF1_PIN", "RF2_ADD",
							 * "RF2_CTY", "RF2_STE", "RF2_CTRY", "RF2_PIN", "RF_TEL_NO", "RF1_TEL_NO",
							 * "RF2_TEL_NO", "RF_M_TEL_NO", "RF1_M_TEL_NO", "RF2_M_TEL_NO", "BR_ORG_NME",
							 * "BR_ORG_CD", "BR_ADD", "BR_CTY", "BR_STE", "BR_CTRY", "BR_PIN"
							 */ };
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
			// hour="AM";
			QrtzEmailConfig mailconfig = (QrtzEmailConfig) osourceTemplate.queryForObject(
					"SELECT *,DATE_FORMAT(NOW(),'%d-%b-%y') todate,DATE_FORMAT(date(NOW() - INTERVAL 1 DAY),'%d-%b-%y') fromdate FROM email_config",
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
					+ "END BRNCH_RGN from NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app left join NEO_CAS_LMS_SIT1_SH.\"Branch\" br on (app.\"Branch Code\"=br.\"Branch Code\") where app.\"Application Number\" is not null and app.\"Sanction Date\" is not null and app.\"Product Type Code\" is not null "
					+ " and app.VERDICT_DATE between TO_TIMESTAMP ('25-Jan-23 09:00:00', 'DD-Mon-RR HH24:MI:SS') and TO_TIMESTAMP ('25-Jan-23 15:59:59', 'DD-Mon-RR HH24:MI:SS')  and app.\"Application Number\" in (select \"Application Number\" from NEO_CAS_LMS_SIT1_SH.\"Individual Customer\" where \"Applicant Type\"='Primary Applicant'  and  \"First Name\" is not null) ";

			String dbapp = "SELECT applicationnumber from hunter_job_application";

			List<String> dbapplist = osourceTemplate.queryForList(dbapp, String.class);
			dbapplist = null;
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

			System.out.println("before fetch query");
			fetchapplication = q.getResultList();
			System.out.println("before fetch query");
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
								+ " and  \"First Name\" is not null and \"Application Number\" in ('" + appNo + "')",
						Tuple.class);
				@SuppressWarnings("unchecked")
				List<Tuple> mainApplicant = q.getResultList();

				System.out.println(appNo + " data size " + mainApplicant);

				if (mainApplicant != null && !mainApplicant.isEmpty() && mainApplicant.size() > 0) {
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
								"select  app.\"Application Number\",adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Residential Address' and app.\"Application Number\" in ('"
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
								"select  app.\"Application Number\",adds.\"Customer Number\", SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Addresstype\"='Permanent Address' and app.\"Application Number\" in ('"
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
								"select SUBSTR(COALESCE(adds.\"Property Address 1\", '')||COALESCE(adds.\"Property Address 2\", '')||COALESCE(adds.\"Property Address 3\", ''),1,480) \"ADD\" , adds.\"Property City\" CTY,adds.\"Property State\" STE,'India' CTRY,adds.\"Property Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Property Details\" adds where adds.\"Property City\" is not null and adds.\"Property State\" is not null and adds.\"Property Pincode\" is not null and \"Application Number\" in ('"
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
								"select  app.\"Application Number\",case when adds.\"Occupation Type\" like 'Self Employed' then adds.\"Organization Name\" else adds.\"Employer Name\" end ORG_NME,case when UPPER(adds.\"Industry\") like 'AGRICULTURE' then 'AGRICULTURE' \r\n"
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
										+ "end EMP_IND from NEO_CAS_LMS_SIT1_SH.\"Employment Details\" adds left join NEO_CAS_LMS_SIT1_SH.APPLICATION_NEWPROD app  on (adds.\"Customer Number\"=app.\"Customer Number\") where adds.\"Occupation Type\" in ('Salaried','Self Employed') and (adds.\"Organization Name\" is not null or adds.\"Employer Name\" is not null) and app.\"Application Number\" in ('"
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
										"select SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN \r\n"
												+ "from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds\r\n"
												+ "where adds.\"Addresstype\"='Office/ Business Address' and adds.\"Customer Number\" in ('"
												+ custNo + "')",
										Tuple.class);

								@SuppressWarnings("unchecked")
								List<Tuple> employeraddress = q.getResultList();

								List<ObjectNode> employeraddressJsom = _toJson(employeraddress);
								if (employeraddressJsom != null && employeraddressJsom.size() > 0) {
									mainApplicantRAJsom.get(emp).put("MA_EMP_AD", employeraddressJsom.get(0));
									prefix = prefix + "_EMP";
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(
												employeraddressJsom.get(0).get(cols.get(p).replace(prefix + "_", "")));
										if (employeraddressJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employeraddressJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
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
								if (employerTelephone != null && employerTelephone.size() > 0) {
									for (int p = 0; p < cols.size(); p++) {
										System.out.println(employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")));
										if (employerTelephoneJsom.get(0)
												.get(cols.get(p).replace(prefix + "_", "")) != null) {
											row.createCell((short) p).setCellValue(employerTelephoneJsom.get(0)
													.get(cols.get(p).replace(prefix + "_", "")).asText());
										}
									}
									mainApplicantRAJsom.get(emp).put("MA_EMP_BT", employerTelephoneJsom.get(0));
								}
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
								"select  adds.\"Customer Number\",SUBSTR(COALESCE(adds.\"Address 1\", '')||COALESCE(adds.\"Address 2\", '')||COALESCE(adds.\"Address 3\", ''),1,480) \"ADD\" , \"City\" CTY,\"State\" STE,'India' CTRY,\"Pincode\" PIN from NEO_CAS_LMS_SIT1_SH.\"Address Details\" adds where adds.\"Addresstype\"='Residential Address' and adds.\"Customer Number\" in ('"
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
						if (brokerAddress != null && brokerAddress.size() > 0) {
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
								+ appNo + "') and NAME is not null and rownum<4",
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
										+ appNo + "') and adds.\"Address 1\" is not null and rownum<4",
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
										+ appNo + "') and \"Mobile Number\" is not null and rownum<4",
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
			System.out.println(batch);
			ObjectMapper xmlMapper = new XmlMapper();
			String xml = xmlMapper.writeValueAsString(batch);

			xml = xml.replace("<ObjectNode>", "").replace("</ObjectNode>", "");
			xml = xml.replace("<JAS>", "").replace("</JAS>", "");
			xml = xml.replace("<RFS>", "").replace("</RFS>", "");
			xml = xml.replace("<MADOC>", "").replace("</MADOC>", "");
			xml = xml.replace("<JADOC>", "").replace("</JADOC>", "");

			String createXml = "<BATCH xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"urn:mclsoftware.co.uk:hunterII\">"
					+ xml + "</BATCH>";
			System.out.println(createXml);
			String sql = "SELECT `nextFileSequence`(1) from dual";

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

}
