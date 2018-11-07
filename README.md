"# JobTalentPipeline" 
## Real Time Job Market Analytics with Indeed

# Description
Use of in-depth, real-time, and localized labor market data allows workforce agencies to increase visibility into local job market needs and helps educators to foster student success.

This script includes collecting online job postings and resumes that provide an important compliment to traditional survey-based labor market data from government agencies.

# Why Collect Resume Data?
Resumes provide a wonderful lens for looking at how jobs match with degrees, training, and previous work experience. Other labor datasets do not provide the same detailed insight about skill transfer and real world career development. That’s because resumes provide a longitudinal analysis that allows you to track how workers progress or stagnate once they start their careers.

# Why Collect Online Job Postings?
The government already collects and distributes a variety of employment and unemployment data by occupation and industry, however online job postings can offer an in depth understanding of skills in demand and education requirements at a granular geographic, industry, and occupation level.  Currently some analysis on online job ads has been incorporated into WA government publicly available datasets, but these are general top 25 lists.  Additionally publicly available datasets for typical education and certification requirements by occupation are only at the national level.  These current datasets do not provide the detailed insights at an occupation and industry level that are needed for workforce agencies to map real-time labor force skills and employer needs at the city level.


# Identify Skill Gaps
This script provides the following metrics:
* The average length of time that a job goes unfulfilled for a certain occupation
* The skills and education background of people applying in your region
* Real life career transitions that affect the skills gap

# Credentials Gap
This script provides the following metrics:
* Look at the credential requirements of employers in your city
* See the average time to fill a job posting for the same occupation with different credential requirements

# Process

## Job Openings
The Indeed API provides us with information about the company, job title, days since a job was posted, the location, time commitment, if the posting has expired, and a key to find the job again later to see if it is still posted. 
Then we visit each of the URLs listed in our results to grab the job description information.  From this unstructured text we use natural language processing methods to extract the education requirements, years of experience requested, and the salary. 
Each search is tagged to a specific Standard Occupational Classification code.

## Job Applicants
We search the Indeed API resume data by current position, years of work experience, education within a certain geographic radius around a city or place. 
This provides us with their current position, location, company, education level, school attended, previous title 
Then we visit all of the URLs in our results and run the resumes through a text classification system that looks for place names and pronouns to identify previous work locations. We can also identify the years worked and the last time someone worked.


