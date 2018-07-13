package functions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import config.DBConfig;
import model.JobAd;

public class DBReader {
	private Connection connection;

	public DBReader() {
		try {
			Class.forName(DBConfig.DRIVER);
			connection = DriverManager.getConnection(DBConfig.URL+DBConfig.DB_NAME, DBConfig.USER_NAME,DBConfig.PASSWORD);
		}catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}

	public ArrayList<JobAd> getAllJobAds(){
		ArrayList<JobAd> jobAds = new ArrayList<>();
		try {
			Statement retrieveJobStatement = connection.createStatement();
			PreparedStatement retrieveJobParagraphs = connection.prepareStatement(DBConfig.GET_PARAGRAPHS,ResultSet.TYPE_FORWARD_ONLY);
			ResultSet jobs = retrieveJobStatement.executeQuery(DBConfig.GET_JOBS);
			while(jobs.next()){
				retrieveJobParagraphs.setInt(1,jobs.getInt(1));
				ResultSet paragraphs = retrieveJobParagraphs.executeQuery();
				String paragraph = "";
				while(paragraphs.next()) {
					paragraph+=paragraphs.getString(4)+" ";
				}
				jobAds.add(new JobAd(jobs.getString(1), paragraph));
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return jobAds;
	}

	public ArrayList<JobAd> getAllJobParagraphs(int personId,boolean NOTFlag){
		ArrayList<JobAd> jobDescriptions = new ArrayList<>();
		PreparedStatement retrieveJobParagraphs;
		try {
			if(personId>0){
				if(!NOTFlag){
					retrieveJobParagraphs = connection.prepareStatement(DBConfig.GET_SELECTED_PARAGRAPHS+DBConfig.NOT_FOR_PERSON,ResultSet.TYPE_FORWARD_ONLY);
					retrieveJobParagraphs.setInt(1, personId);
				}
				else{
					retrieveJobParagraphs = connection.prepareStatement(DBConfig.GET_SELECTED_PARAGRAPHS+DBConfig.FOR_PERSON,ResultSet.TYPE_FORWARD_ONLY);
					retrieveJobParagraphs.setInt(1, personId);
				}
			}
			else{
				retrieveJobParagraphs = connection.prepareStatement(DBConfig.GET_SELECTED_PARAGRAPHS,ResultSet.TYPE_FORWARD_ONLY);
			}
			ResultSet paragraphs = retrieveJobParagraphs.executeQuery();
			while(paragraphs.next()) {
				boolean flag=false;
				for(JobAd jobAd : jobDescriptions){
					String id = paragraphs.getString(1);
					if(id.equals(jobAd.getId())){
						jobDescriptions.remove(jobAd);
						jobAd.concatenateParagraph(paragraphs.getString(2));
						jobDescriptions.add(jobAd);
						flag=true;
						break;
					}
				}
				if(!flag) {
					jobDescriptions.add(new JobAd(paragraphs.getString(1), paragraphs.getString(2)));
				}
			}
		}catch (SQLException e) {
			e.printStackTrace();
		}
		return jobDescriptions;
	}

	public void closeConnection() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}