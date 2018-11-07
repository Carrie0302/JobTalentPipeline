"""
Created on Fri July 5 13:18:31 2018

Research into the Indeed API for the Talent Pipeline
This program uses:  the Indeed API for intitial jobs and resume lists, 
Rapid Automatic Keyword Extraction (RAKE), Web scraping URLs with job detials,
NLP stop word removal, supplementary information on potential similar
jobs comes from  The Open Skills Project API

@author: carrie

"""
import pandas as pd
import difflib, os
import numpy as np
import datetime, sqlalchemy
import requests, urllib.request, shutil, zipfile
import datetime, os, time
from bs4 import BeautifulSoup
import datetime, os, json
import urllib.parse as urlparse
from rake_nltk import Rake
from nltk.corpus import stopwords
import re

APPNAME = 'please fill in'
token = 'please fill in'
secret = 'please fill in'
publisher = 'please fill in'


class IndeedAPIResumesandJobs:
   
    #Todays Date
    now = datetime.datetime.now()
    formatYMD = now.strftime("%Y-%m-%d")
    formatTime = now.strftime("%Y-%m-%d %H:%M")
    print("Running Indeed API: {0}".format(formatTime))
    
    #Main Directory
    dir_path = os.path.dirname(os.path.realpath(__file__))

    def error_reporting(self, method_name):
        log = open("Error_Data_Indeed.txt","a")
        log.write("Error calling the method {0}. Location:IndeedAPI_TalentPipeline.py  Date: {1} \n".format(method_name, self.formatTime))


    def query_reporting(self, method,  result_count,  title, location, days_since_posted, query):
        log = open("Succesfull_Query_Result_Logs.txt","a")
        log.write("Method {0} \t  Results: {1} \t  Date: {2} \n\t\tTitle:{3} \t  Location:{4} \t  Other Args:{5} \n\t\tQuery: {6} \n".format(method,  result_count, self.formatTime, title, location, days_since_posted, query ))


    def send_to_sql(self, dataFrame, tableName, updateType, index):
        #JETPACK
        params1 = urllib.parse.quote_plus("Driver={SQL Server};SERVER=JETPACK\CAISQL2;DATABASE=CAIData_EconomicIndicators")
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params1)
        
        if updateType != "ReplaceAllVals":
            if index == False:
                dataFrame[:].to_sql(tableName, engine, schema = 'ESD', index=False, if_exists='append', chunksize=100)
            else:
                dataFrame[:].to_sql(tableName, engine, schema = 'ESD', if_exists='append', chunksize=100)


    def read_occupations(self):
        """Read Occupation Names"""
        file_path = "{0}//Lookup Tables//Occupations_and_Codes.xlsx".format(self.dir_path)
        detailed_occupations = pd.read_excel(file_path, sheet_name="detailed")
        print(detailed_occupations['OCC_TITLE'])
        return detailed_occupations


    def call_indeed_resumes(self):
        """Call resume data, You can specify """
        params = {
            'q' : title,
            'l' : location,
            'jt' : hours_a_week,
            'sort': sort_type,
            'userip' : "1.2.3.4",
            'useragent' : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
        }
        url = r"https://auth.indeed.com/resumes?q=title%3A%28General+Manager%29&l=Seattle%2C+WA&access_token={0}".format(token)
        page = requests.get(url)
        save_result = page.json()
        print(save_result)


    def job_search_by_hours_a_week(self, title, location, hours_a_week="fulltime", sort_type='relevance'):
        """Call jobs data, You can specify fulltime, parttime, contract, internship, or temporary."""

        params = {
            'q' : title,
            'l' : location,
            'jt' : hours_a_week,
            'sort': sort_type,
            'userip' : "1.2.3.4",
            'useragent' : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
        }

        url = r"http://api.indeed.com/ads/apisearch?publisher={0}&format=json&q={1}&l={2}&fromage={3}&sort={4}&userip=1.2.3.4&useragent=Mozilla/%2F4.0%28Firefox%29&v=2".format(publisher, params['q'], params['l'], params['jt'], params['sort'])
        print(url)
        page = requests.get(url)
        json_text = json.loads(page.text)

        page = requests.get(url)
        json_text = json.loads(page.text)
        result_count = json_text['totalResults']
        other_args = "{0} hours a week".format(hours_a_week)
        self.query_reporting('job_searches_by_date',  result_count, title, location, other_args, url)

        # table = pd.read_json(json.dumps(json_text))
        # print(table)
        # table.to_csv("CAI.IndeedAPI Job {0} - Location {1} - Position {2} .{3}.csv".format(title, location, hours_a_week, self.formatYMD))

    def iterate_to_end_of_results(self, result_count):
        for i in range(0, result_count, 10):
            print("Start: {0}".format(i))
            print("\t End:{0}".format(i+10))


    def get_url(self, url):
        page = requests.get(url)
        json_text = json.loads(page.text)
        result_count = json_text['totalResults']
        return result_count, json_text



    def find_related_jobs(self, title):
        """http://api.dataatwork.org/v1/spec/#!/default/get_jobs_id_related_jobs
        The Open Skills Project 
        GET /jobs/{id}/related_jobs
        Use the The O*NET SOC Code or UUID of the job title to retrieve similar job titles
        """
        t = 0


    def skills_call_api(self, searchTerm):
        """http://api.dataatwork.org
        The Open Skills Project is a public-private partnership lead by the University of Chicago focused on providing a dynamic, up-to-date, locally-relevant, and normalized taxonomy 
        of skills and jobs that builds on and expands on the Department of Labor’s O*NET data resources. It’s aim is to improve our understanding of the labor market 
        and reduce frictions in the workforce data ecosystem by enabling a more granular common language of skills among industry, academia, government, and nonprofit organizations."""
        t = 0


    def open_job_detail_log(self, jobkey):
        path =  "{0}/Job Details {1}/Job ['{2}'].txt".format(self.dir_path, self.formatYMD, jobkey)
        log = open(path,"r")
        print(log)



    def findYearsOfExperience(self, textDescription):
        # Lets use a regular expression to find years of Experience
        numbers = {1: "one", 2: "two", 3: "three"}
        allExperience = []

        #Find required years and skill associated with: 9+ years of SQL or C++ experience
        regexOne = r"(\d+)\+? years of (.+)? experience"
        yearsExperience =  re.findall(regexOne, textDescription)
        allExperience.extend(yearsExperience)
        print(yearsExperience)

        #Find required years and skill associated with: 2+ years of experience with Java
        regexTwo = r"(\d+)\+? years of experience (.+)"
        yearsExperienceTwo =  re.findall(regexTwo, textDescription)
        allExperience.extend(yearsExperienceTwo)
        print(yearsExperienceTwo)

        #Find required years and skill associated with: 2 to 3 years of experience with computers
        #Strip out the later year
        regexThree = r"(\d+ to|\d+ -) (\d+) years of experience (.*)"
        yearsExperienceThree =  re.findall(regexThree, textDescription)
        if( len(yearsExperienceThree) > 0):
            cleanCondense = [( re.sub("[^0-9]", "", t[0]) , t[2] ) for t in yearsExperienceThree]
            allExperience.extend(cleanCondense)
            print(cleanCondense)

        #re.sub("[^0-9]", "",t[0])



        # #Find required years and skill associated with: at least 5 years SyteLine developing experience
        # regexFour = r"at least (\d+) years o?f? (.*)"
        # yearsExperienceFour =  re.findall(regexFour, textDescription)
        # allExperience.extend(yearsExperienceFour)
        # print(yearsExperienceFour)
        

        #Find required years and skill associated with: Mongoose framework: 5 years  
        regexFive = r"([a-zA-Z_ \r\f\v]*): (\d+) years" # "(\D*): (\d+) years"
        yearsExperienceFive =  re.findall(regexFive, textDescription)
        if( len(yearsExperienceFive) > 0):
            reOrder = [(t[1], t[0].strip() ) for t in yearsExperienceFive]
            allExperience.extend(reOrder)
            print(reOrder)

        exp = list(set(allExperience))
        for e in exp:
            print(e)

        #To print full match
        # matches = re.findall(regex, textDescription)
        # for match in matches:
        #     # This will print:
        #     #   June 24
        #     #   August 9
        #     #   Dec 12
        #     print("Full match: %s" % (match))

        return exp



    #This Converts the skills to long form
    def explore_years_of_experience( self, title, location, days_since_posted=30, date='000'):
        if( date == '000' ):
            date = self.formatYMD

        #Export Temp wide table
        df = pd.read_csv("CAI.IndeedAPI Jobs Detailed {0} - {1}.csv".format(title, date))
        df['years_exp_by_skill'] =  df['job-content'].apply(self.findYearsOfExperience)
        #df.to_csv("CAI.IndeedAPI Jobs Years of Experience {0} - Location {1} - Days Since Post {2} .{3}.csv".format(title, location, days_since_posted, date))

        #Transpose data so that only the title years and skills are listed long wise
        skillsLong = []
        for index, row in df.iterrows():
            skills_per_job = row['years_exp_by_skill']
            for s in skills_per_job:
                newRow = [ row['jobkey'], row['jobtitle'], s[0], s[1] ]
                skillsLong.append(newRow)
            #print( row['jobkey'], row['jobtitle'], row['years_exp_by_skill'])

        #Export Long Skills Table
        labels = ['years_exp_by_skill', 'skill', 'jobkey',	'jobtitle']
        dfLong = pd.DataFrame.from_records(skillsLong, columns=labels)
        dfLong.to_csv("CAI.IndeedAPI Jobs LONG Years of Experience {0} - Location {1} - Days Since Post {2} .{3}.csv".format(title, location, days_since_posted, date))
        print(skillsLong)




    def extractKeywords(self, textDescription):
        """Get the keyword phrases from the descriptions using NLP"""
        r =  Rake()
        r.extract_keywords_from_text(textDescription)
        results = r.get_ranked_phrases() # To get all keyword phrases ranked highest to lowest.
        result_scores = r.get_ranked_phrases_with_scores()
        print(results)
        return results



    def explore_results( self, title, location, days_since_posted=30):
        df = pd.read_csv("CAI.IndeedAPI Jobs Detailed {0} - {1}.csv".format(title, self.formatYMD))
        #df['find_quals'] = df['jobkey'].apply(self.open_job_detail_log)
        df['find_keywords'] =  df['job-content'].apply(self.extractKeywords)
        df.to_csv("CAI.IndeedAPI Jobs Keywords {0} - Location {1} - Days Since Post {2} .{3}.csv".format(title, location, days_since_posted, self.formatYMD))
        print(df)



    def get_url_detail(self, url):
        page = requests.get(url)
        data = page.text
        soup = BeautifulSoup(data, 'lxml')
        time.sleep(1)

        #Strip out the recommended jobs section which would mess with the nlp
        job_clean = soup.find(id="job_summary")
        if job_clean is None:
            print("Slow connection wait 5 seconds")
            time.sleep(5)
            job_clean = soup.find("span", {"class": "summary"})

        if job_clean is None:
            time.sleep(10)
            print("Slow connection wait 10 seconds")
            job_clean = soup.find("table", {"id": "job-content"})
        
        print(url)
        #print(job_clean)
        #When the connection is slow
        if( job_clean is None):
            job_text = job_clean
        else:
            job_text = job_clean.text

        #Save all job detail to a log file
        newpath = '{0}/Job Details {1}'.format(self.dir_path, self.formatYMD ) 
        if not os.path.exists(newpath):
            os.makedirs(newpath)

        #Parse url for job id
        parsed = urlparse.urlparse(url)
        jk = urlparse.parse_qs(parsed.query)['jk']

        log = open("{0}/Job {1}.txt".format(newpath, jk),"w+")
        log.write(str(job_text))

        return job_text


    def job_details( self,  title, location, days_since_posted=30, sort_type='relevance'):
        df = self.job_searches_by_date( title, location, days_since_posted, sort_type)
        df['job-content'] = df['url'].apply(self.get_url_detail)
        #print("Save as CSV")
        df.to_csv("CAI.IndeedAPI Jobs Detailed {0} - {1}.csv".format(title, self.formatYMD))



    def job_searches_by_date( self, title, location, days_since_posted=30, sort_type='relevance'):
        """Number of days since a job was published. If you specify 15, for example, the API searches jobs published only within the last 15 days.
        Use pagination to traverse results, Start returning jobs at this result number, beginning at 0."""

        params = {
            'q' : title,
            'l' : location,
            'fromage' : days_since_posted,
            'sort': sort_type,
            'userip' : "1.2.3.4",
            'useragent' : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2)"
        }
        #&st=jobsite
        url = r"http://api.indeed.com/ads/apisearch?publisher={0}&format=json&q={1}&l={2}&fromage={3}&sort={4}&start={5}&userip=1.2.3.4&useragent=Mozilla/%2F4.0%28Firefox%29&v=2".format(publisher, params['q'], params['l'], params['fromage'], params['sort'], 0)
        print(url)        

        #Call URL
        result_count, json_text = self.get_url(url)
        first_results = pd.read_json(json.dumps(json_text['results']))
        read_input = [first_results]

        #Save reult report in log
        other_args = "{0} Days Since Posted".format(days_since_posted)
        self.query_reporting('job_searches_by_date',  result_count, title, location, other_args, url)

        for i in range(0, result_count, 10):
            print("Start: {0}".format(i))
            url = r"http://api.indeed.com/ads/apisearch?publisher={0}&format=json&q={1}&l={2}&fromage={3}&sort={4}&start={5}&userip=1.2.3.4&useragent=Mozilla/%2F4.0%28Firefox%29&v=2".format(publisher, params['q'], params['l'], params['fromage'], params['sort'], i)
            print(url)        
            _, json_text = self.get_url(url)
            table = pd.read_json(json.dumps(json_text['results']))
            table.drop_duplicates(['jobkey'], keep='first', inplace=True)
            #print(table)
            read_input.append(table)

        new_df = pd.concat(read_input)
        new_df.drop_duplicates(['jobkey'], keep='first', inplace=True)
        new_df.to_csv("CAI.IndeedAPI Jobs {0} - Location {1} - Days Since Post {2} .{3}.csv".format(title, location, days_since_posted, self.formatYMD))
        return new_df




#Get resumes       
#resumes = IndeedAPIResumesandJobs()
#resumes.call_indeed_resumes()

# #Download New Jobs
jobs =IndeedAPIResumesandJobs()
#job_name = 'gis+analyst'
job_name = 'SDE'
#jobs.job_details( job_name, "Seattle, WA", 20, 'date')
# #jobs.job_search_by_hours_a_week('developer', "Seattle, WA", 'fulltime', 'date')
# #jobs.iterate_to_end_of_results(55)

#Explore job details with NLP
#jobs.explore_results( job_name, "Seattle, WA", 20)
jobs.explore_years_of_experience(job_name, "Seattle, WA", 20 )





# jobs.findYearsOfExperience( """ Job Description
#             Are you passionate about learning? Do you live and breathe data? Amazonâ€™s Physical Stores team is seeking a program manager to lead research and data acquisition to help us source and analyze data, help us learn, and develop insights about our customers, our communities, and our business.

#             You will work cross functionally with real estate, modeling, GIS, finance, and other teams to develop requirements around data acquisition and analytics. You will own the identification of new data sources, and the relationships with key data vendors and outside consultants. You will track trends in data and identify innovative new products that can help us better understand our customers and the cities where we operate. You will be responsible for negotiating acquisition and managing our vendor relationships.
#             7 - 99 years of experience with catering 
#             You should be comfortable working with large data sets. You should be familiar with typical data sets (demographic, market, social media, etc.) and comfortable working across multiple formats. You should have experience in sourcing and management of external vendors.
#             The ideal candidate will have the ability to synthesize information to produce insights, and to explain complex data to a non-technical audience. You will be a self-driven analyst who proactively seeks to build knowledge of the data and analytics industry and will become a thought leader to the enterprise.
#             Basic Qualifications
            
#             Mongoose framework: 5 yearsApplication Development: 8 yearsSyteLine / CloudSuite Industrial: 5 years
#             10 years of C++ experience
#             5+ years developing software solutions in large scale (over 10,000 users) global environments
#             1 to 3 years of experience with Dinasours. 1 to 2 years of experience with Leopards
#             5+ years developing software solutions in large scale (over 10,000 users) global environments
#             2+ years of working with large distributed systems
#             BA/BS Degree in Statistics, Mathematics, Computer Science, GIS, Geography, Predictive Modeling, Economics or a related degree.
#             3+ years of experience working with large datasets.
#             7 years of software engineering or related experience.
#             2 years of experience with data mining, SQL, data visualizations, Postgre SQL database experience, and/or other databases such as Oracle, SQLite, SQL Server, Redshift.
#             5+ years of field experience.
#             12 years of field experience.
#             MUST have at least 5 years SyteLine developing experience. Mongoose and Visual are an absolute requirement.
#             Experienced working with external vendors and managing contracts.
#             Track record articulating business questions and using quantitative techniques to arrive at a solution using available data.
#             Preferred Qualifications
#             Masters Degree.
#             at least 5 years SyteLine developing experience
#             ESRI or other GIS and spatial database experience.
#             Experience working with customer data in a retail environment.
#             Excellent verbal and written communication skills with the ability to effectively advocate technical solutions to leadership.
#             Demonstrable track record of dealing well with ambiguity, prioritizing business needs, and delivering results in a dynamic environment.
#             Demonstrated track record of creative problem solving; think big, start small, grow fast""")



# jobs.extractKeywords( """ Job Description
#             Are you passionate about learning? Do you live and breathe data? Amazonâ€™s Physical Stores team is seeking a program manager to lead research and data acquisition to help us source and analyze data, help us learn, and develop insights about our customers, our communities, and our business.

#             You will work cross functionally with real estate, modeling, GIS, finance, and other teams to develop requirements around data acquisition and analytics. You will own the identification of new data sources, and the relationships with key data vendors and outside consultants. You will track trends in data and identify innovative new products that can help us better understand our customers and the cities where we operate. You will be responsible for negotiating acquisition and managing our vendor relationships.

#             You should be comfortable working with large data sets. You should be familiar with typical data sets (demographic, market, social media, etc.) and comfortable working across multiple formats. You should have experience in sourcing and management of external vendors.
#             The ideal candidate will have the ability to synthesize information to produce insights, and to explain complex data to a non-technical audience. You will be a self-driven analyst who proactively seeks to build knowledge of the data and analytics industry and will become a thought leader to the enterprise.
#             Basic Qualifications
#             BA/BS Degree in Statistics, Mathematics, Computer Science, GIS, Geography, Predictive Modeling, Economics or a related degree.
#             3+ years of experience working with large datasets.
#             3+ years of experience with data mining, SQL, data visualizations, Postgre SQL database experience, and/or other databases such as Oracle, SQLite, SQL Server, Redshift.
#             Experienced working with external vendors and managing contracts.
#             Track record articulating business questions and using quantitative techniques to arrive at a solution using available data.
#             Preferred Qualifications
#             Masters Degree.
#             ESRI or other GIS and spatial database experience.
#             Experience working with customer data in a retail environment.
#             Excellent verbal and written communication skills with the ability to effectively advocate technical solutions to leadership.
#             Demonstrable track record of dealing well with ambiguity, prioritizing business needs, and delivering results in a dynamic environment.
#             Demonstrated track record of creative problem solving; think big, start small, grow fast""")


