import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class PentahoFromMySQL 
{
	private static final String PENTAHO_REPO_NAME = "Pentaho_Repository";
	private static final String PENTAHO_REPO_TYPE = "MySql";
	private static final String PENTAHO_REPO_SYS = "twitternewyork.c3lq996wo33i.us-east-1.rds.amazonaws.com";
	
	private static final String PENTAHO_REPO_DB = "pentaho_repository_new";
	private static final String PENTAHO_REPO_PORT = "3306";
	private static final String PENTAHO_REPO_USER = "dragonfly";
	private static final String PENTAHO_REPO_PASS = "datafactory";
	
	public String[] fetchPentahoTransformationsFromRepo(String userName, String password, String strDirectory) throws KettleException
	{
		 KettleEnvironment.init();		 
		 KettleDatabaseRepository repository = new KettleDatabaseRepository();		 
		 DatabaseMeta databaseMeta = new DatabaseMeta(PENTAHO_REPO_TYPE, PENTAHO_REPO_TYPE, "", PENTAHO_REPO_SYS , PENTAHO_REPO_DB, PENTAHO_REPO_PORT, PENTAHO_REPO_USER, PENTAHO_REPO_PASS );
		 
		 KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta( PENTAHO_REPO_NAME, PENTAHO_REPO_NAME , "Transformation description", databaseMeta );
		 
		 repository.init( kettleDatabaseMeta );
		 repository.connect( userName, password );
		 
		 RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();
		 
		 if(strDirectory!=null && !"".equals(strDirectory))
		 {
			 directory = directory.findDirectory(strDirectory);
		 }		 	
		 
		 //System.out.println("Directory ObjectID:"+directory.getObjectId());
		 String strTransList[] = repository.getTransformationNames(directory.getObjectId(), true);
		 System.out.println("List of Transformation Names:");
		 for(String str: strTransList)
		 {
			System.out.println(str);
		 }
		
		 repository.disconnect();
		 return strTransList;
	}
	
	public String[] fetchPentahoJobsFromRepo(String userName, String password, String strDirectory) throws KettleException
	{
		 KettleEnvironment.init();		 
		 KettleDatabaseRepository repository = new KettleDatabaseRepository();		 
		 DatabaseMeta databaseMeta = new DatabaseMeta(PENTAHO_REPO_TYPE, PENTAHO_REPO_TYPE, "", PENTAHO_REPO_SYS , PENTAHO_REPO_DB, PENTAHO_REPO_PORT, PENTAHO_REPO_USER, PENTAHO_REPO_PASS );
		 
		 KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta( PENTAHO_REPO_NAME, PENTAHO_REPO_NAME , "Job description", databaseMeta );
		 
		 repository.init( kettleDatabaseMeta );
		 repository.connect( userName, password );
		 
		 RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();		 
		 
		 if(strDirectory!=null && !"".equals(strDirectory))
		 {			 
			 directory = directory.findDirectory(strDirectory);
		 }		 	
		
		 String strJobList[] = repository.getJobNames(directory.getObjectId(), true);
		 
		 System.out.println("List of Job Names:");
		 for(String str: strJobList)
		 {
			System.out.println(str);
		 } 
		
		 repository.disconnect();
		 return strJobList;
	}
	
	public void executePentahoTransFromRepo(String userName, String password, String strDirectory, String transName) throws KettleException
	{
		 KettleEnvironment.init();		 
		 KettleDatabaseRepository repository = new KettleDatabaseRepository();		 
		 DatabaseMeta databaseMeta = new DatabaseMeta(PENTAHO_REPO_TYPE, PENTAHO_REPO_TYPE, "", PENTAHO_REPO_SYS , PENTAHO_REPO_DB, PENTAHO_REPO_PORT, PENTAHO_REPO_USER, PENTAHO_REPO_PASS );
		 
		 KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta( PENTAHO_REPO_NAME, PENTAHO_REPO_NAME , "Transformation description", databaseMeta );
		 
		 repository.init( kettleDatabaseMeta );
		 repository.connect( userName, password );
		 
		 RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();
		 
		 if(strDirectory!=null && !"".equals(strDirectory))
		 {
			 directory = directory.findDirectory(strDirectory);
		 }		 	
		 
		 TransMeta transformationMeta = repository.loadTransformation(transName, directory, null, true, null ) ;
		 Trans trans = new Trans(transformationMeta);
		 //trans.setParameterValue( parameterName, parameterValue);
		 trans.setLogLevel(LogLevel.NOTHING);
		 trans.execute(null); // You can pass arguments instead of null.
		 
		 trans.waitUntilFinished();
		 
		 if(trans.getErrors() > 0 ) 
		 {                     
              System.out.println("Transformation "+transName +" ended-up with errors");
		 }
		 else
		 {
			  System.out.println("Transformation "+transName +" executed successfully"); 
		 }
		
		 repository.disconnect();		 
	}
	
	public void executePentahoJobFromRepo(String userName, String password, String strDirectory, String jobName) throws KettleException
	{
		 KettleEnvironment.init();		 
		 KettleDatabaseRepository repository = new KettleDatabaseRepository();		 
		 DatabaseMeta databaseMeta = new DatabaseMeta(PENTAHO_REPO_TYPE, PENTAHO_REPO_TYPE, "", PENTAHO_REPO_SYS , PENTAHO_REPO_DB, PENTAHO_REPO_PORT, PENTAHO_REPO_USER, PENTAHO_REPO_PASS );
		 
		 KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta( PENTAHO_REPO_NAME, PENTAHO_REPO_NAME , "Job description", databaseMeta );
		 
		 repository.init( kettleDatabaseMeta );
		 repository.connect( userName, password );
		 
		 RepositoryDirectoryInterface directory = repository.loadRepositoryDirectoryTree();
		 
		 if(strDirectory!=null && !"".equals(strDirectory))
		 {
			 directory = directory.findDirectory(strDirectory);
		 }		 	
		 
		 JobMeta jobMeta = repository.loadJob(jobName, directory, null, null);
		 Job job = new Job(repository, jobMeta);		 
		 job.setLogLevel(LogLevel.NOTHING);
		 
		 job.run(); 
		 job.waitUntilFinished();
		 
		 if (job.getErrors() != 0) 
		 {
			 System.out.println("Job "+jobName +" ended-up with errors");
		 }	
		 else
		 {
			  System.out.println("Job "+jobName +" executed successfully"); 
		 }
		
		 repository.disconnect();		 
	}
	
	public static void main(String args[]) throws KettleException
	{
		PentahoFromMySQL pmySQL = new PentahoFromMySQL();
		System.getProperties().setProperty("KETTLE_PLUGIN_BASE_FOLDERS", "C:/Pentaho/design-tools/data-integration/plugins");
		String[] listTrans =  pmySQL.fetchPentahoTransformationsFromRepo("Sudheer", "Sudheer", "Testing");
		String[] listJobs =  pmySQL.fetchPentahoJobsFromRepo("Sudheer", "Sudheer", "Testing");
		
		/*for(String transName: listTrans)
		{
			System.out.println("Start executing "+transName+ " transformation...");
			pmySQL.executePentahoTransFromRepo("Sudheer", "Sudheer", "Testing", transName);
		}*/
		
		for(String jobName: listJobs)
		{
			System.out.println("Start executing "+jobName+ " job...");
			pmySQL.executePentahoJobFromRepo("Sudheer", "Sudheer", "Testing", jobName);
		}
	}
}
