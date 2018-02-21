import re
import os
import sys
import time
import json
import boto3
import string
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException

display = Display(visible=0, size=(1600, 1600))
display.start()
browser = webdriver.Chrome()
browser.set_page_load_timeout(60)

wait_time = 30
numpartners = 0
partners_list = [] 
pagesize = 100
page_url = "https://aws.amazon.com/partners/find/results/?size=100&start=0&sort=Relevance&view=List"
max_search_attempt = 5

partners_file_name = "apn-partners.txt"
partner_details_file_name = "apn-partner-details.txt"

table_name = "APNPartnerMaster"

dynamodb = boto3.client('dynamodb', 'us-east-1')

try:
    db_table = dynamodb.create_table(
        AttributeDefinitions=[
            {
                'AttributeName': 'partner_name',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'partner_website',
                'AttributeType': 'S'
            }          
        ],
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'partner_name',
                'KeyType': 'HASH'
            },             
            {
                'AttributeName': 'partner_website',
                'KeyType': 'RANGE'
            }            
        ],        
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        },
        StreamSpecification={
            'StreamEnabled': True,
            'StreamViewType': 'NEW_IMAGE'
        }
    )  
    print("Table {} created".format(db_table["TableDescription"]["TableName"]))
except dynamodb.exceptions.ResourceInUseException:
    print("Table {} already exists".format(table_name))
    
    
    
if os.path.isfile(partners_file_name):
    print("Loading partner names from local file {}".format(partners_file_name))
    partner_file = open(partners_file_name,"r" )
    partnerList = partner_file.readlines()
    partner_file.close()
    for i in range(len(partnerList)):
        line = partnerList[i]
        if line.find("~") > 0:
            partner = re.sub("\n", "", line[line.find("~")+1:])
            if partner != "partner_name":
                partners_list.append(partner)
    numpartners = len(partners_list)   
    print("Total {} partner names loaded from local file {}".format(len(partners_list), partners_file_name))   

while numpartners <= 0:
    try:
        browser.get(page_url)
        WebDriverWait(browser, wait_time).until(EC.presence_of_element_located((By.CLASS_NAME, 'psf-results')))
        partner_count_div = browser.find_elements_by_class_name("psf-results")
        partner_count_div_text = partner_count_div[0].text
        numpartners = int(partner_count_div_text[partner_count_div_text.find("of ")+3:partner_count_div_text.find(" Results")])
    except TimeoutException:
        print("Loading  AWS Partner Solutions Finder Search Page took too long, trying again in {} seconds".format(wait_time))
        time.sleep(wait_time)     
        
if len(partners_list) <=0:
    print("Finding {} APN Partners from  AWS Partner Solutions Finder".format(numpartners))    
    for i in range(0,numpartners,pagesize):

        page_url = "https://aws.amazon.com/partners/find/results/?size={}&start={}&sort=Relevance&view=List".format(pagesize,i)
        print("Loading Partner List Page-{} using URL: {}".format(int(i/pagesize)+1, page_url))
        partner_divs = []
        while len(partner_divs) <= 0:
            try:
                browser.get(page_url)
                WebDriverWait(browser, wait_time).until(EC.presence_of_element_located((By.CLASS_NAME, 'psf-partner-name')))
                print("Page-{} Loaded.".format(int(i/pagesize)+1))
                partner_divs = browser.find_elements_by_class_name("psf-partner-name")
                for j in range(len(partner_divs)):
                    partners_list.append(partner_divs[j].text.encode('utf-8'))
                print("{} Partner names captured from Page-{}".format(len(partner_divs), int(i/pagesize)+1))
            except TimeoutException:
                print("Loading Page-{} took too much time, trying again in {} seconds".format(int(i/pagesize)+1, wait_time))
                i = i - pagesize
                time.sleep(wait_time)
    partners_file = open(partners_file_name, "w+")                    
    partners_file.writelines("{}~{}\n".format("id","partner_name"))
    for i in range(len(partners_list)):
        partners_file.writelines("{}~{}\n".format(i+1,partners_list[i]))
    partners_file.close()
    print("Total {} partners captured from AWS Partner Solutions Finder".format(len(partners_list)))   

if not os.path.isfile(partner_details_file_name):
    partner_details_file = open(partner_details_file_name, "w+")                    
    partner_details_file.writelines("{}~{}~{}~{}~{}~{}~{}\n".format("id","partner_name", 
                                                            "partner_website",
                                                            "partner_location",
                                                            "partner_type",
                                                            "partner_description",
                                                            "partner_competencies"))    
    partner_details_file.close()
    
resume = 0

if os.path.isfile(partner_details_file_name):
    partner_details_file = open(partner_details_file_name,"r" )
    partnerList = partner_details_file.readlines()
    partner_details_file.close()
    if len(partnerList) > 0:
        lastline = partnerList[len(partnerList)-1]
        if lastline.find("~") > 0:
            pid = lastline[:lastline.find("~")]
            try:
                resume = int(pid)
            except:
                pass  
            
until = len(partners_list)

print("Starting to search Partner details and Twitter hanldes from position-{}".format(resume+1))
for pid in range(resume, until):
    partner_name = partners_list[pid]
    partner_search_url="https://aws.amazon.com/partners/find/results/?keyword={}".format(re.sub(" ","+",re.sub("/"," ",partner_name)))
    partner_website = ""
    partner_location = ""
    partner_type = ""
    partner_description = ""
    qualification_names = []
    qualification_values = [] 
    partner_competencies = ""
    num_competencies = 0
    partner_search_attempt = 0
    
    while partner_search_attempt < max_search_attempt:
        try:
            print("Searching APN Partner details for {}".format(partner_name))
            browser.get(partner_search_url)
            WebDriverWait(browser, wait_time).until(EC.presence_of_element_located((By.CLASS_NAME, 'psf-partner-name')))
            partner_page_link = None
            
            try:
                partner_page_link = browser.find_element_by_link_text(partner_name)
            except NoSuchElementException:
                print("Partner detail page for {} doesn't exist in APN".format(partner_name))
                
            if partner_page_link is not None:
                ActionChains(browser).move_to_element(partner_page_link).click().perform()
                WebDriverWait(browser, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'psf-card')))
                weburl_divs = browser.find_elements_by_class_name("psf-card")
                
                if len(weburl_divs) > 0:
                    partner_website=weburl_divs[1].text              
                hq_location_divs = browser.find_elements_by_class_name("psf-hq-location")
                if len(hq_location_divs) > 0:
                    partner_location = hq_location_divs[0].text
                partnertype_divs = browser.find_elements_by_class_name("psf-partner-type")
                if len(partnertype_divs) > 0:
                    partner_type = partnertype_divs[0].text                    
                overview_divs = browser.find_elements_by_class_name("psf-overview")
                if len(overview_divs) > 0:
                    partner_description = overview_divs[0].text
                competencies_divs = browser.find_elements_by_class_name("psf-competencies")
                if len(competencies_divs) > 0:
                    competencies_type_headers = competencies_divs[0].find_elements_by_class_name("psf-help")
                    if len(competencies_type_headers) > 0:
                        for competencies_type_header in competencies_type_headers:
                            qualification_names.append(
                                ''.join(
                                    list(filter(lambda x: x in string.printable, competencies_type_header.text))
                                ).strip().replace('\n','|')
                            )
                    competencies_lists = competencies_divs[0].find_elements_by_class_name("psf-competencies")
                    if len(competencies_lists) > 0:
                        for competencies_list in competencies_lists:
                            qualification_values.append(
                                ''.join(
                                    list(filter(lambda x: x in string.printable, competencies_list.text))
                                ).strip().replace('\n','|')
                            )                            
                if len(competencies_divs) > len(qualification_values)+1:
                    competencies_type_headers = competencies_divs[len(qualification_values)+1].find_elements_by_class_name("psf-competency-header")
                    if len(competencies_type_headers) > 0:
                        for competencies_type_header in competencies_type_headers:
                            qualification_names.append(
                                ''.join(
                                    list(filter(lambda x: x in string.printable, competencies_type_header.text))
                                ).strip().replace('\n','|')
                            )
                    competencies_lists = competencies_divs[len(qualification_values)+1].find_elements_by_class_name("psf-competencies")
                    if len(competencies_lists) > 0:
                        for competencies_list in competencies_lists:
                            qualification_values.append(
                                ''.join(
                                    list(filter(lambda x: x in string.printable, competencies_list.text))
                                ).strip().replace('\n','|')
                            )        
                            
                num_competencies = len(qualification_values)
                if len(qualification_names) < num_competencies:
                    num_competencies = len(qualification_names)   
                for c in range(num_competencies):
                    partner_competencies = partner_competencies + qualification_names[c] + " : " + qualification_values[c] + "; "
                    
        except TimeoutException:
            partner_search_attempt = partner_search_attempt + 1
            if partner_search_attempt >= max_search_attempt:
                print("Couldn't find Partner details for {}, try manually!!!".format(partner_name, wait_time))
            else:
                print("Finding Partner detailsfor {} took too much time, trying again in {} seconds...".format(partner_name, wait_time))
            time.sleep(wait_time)  
        else:
            partner_search_attempt = max_search_attempt
        
    partner_details_file = open(partner_details_file_name, "a")  
    partner_details_file.writelines("{}~{}~{}~{}~{}~{}~{}\n".format(pid+1,
                                                                 partner_name.encode('utf-8'), 
                                                                 partner_website, 
                                                                 partner_location.encode('utf-8'), 
                                                                 partner_type, 
                                                                 partner_description.encode('utf-8'),
                                                                 partner_competencies))    
    partner_details_file.close()            
    
    expression_attribute_names = {}
    expression_attribute_values = {}
    update_expression = "SET "
    
    if partner_location != "":
        expression_attribute_names["#PartnerLocation"] = "partner_location"
        expression_attribute_values[":partner_location"] = {"S" : partner_location.encode('utf-8')}
        update_expression = update_expression + "#PartnerLocation = :partner_location,"
    if partner_type != "":
        expression_attribute_names["#PartnerType"] = "partner_type"
        expression_attribute_values[":partner_type"] = {"S" : partner_type}
        update_expression = update_expression + "#PartnerType = :partner_type,"        
    if partner_description != "":
        expression_attribute_names["#PartnerDescription"] = "partner_description"
        expression_attribute_values[":partner_description"] = {"S" : re.sub("\n"," ",partner_description.encode('utf-8').strip())}
        update_expression = update_expression + "#PartnerDescription = :partner_description,"      
    if num_competencies > 0:
        for c in range(num_competencies):            
            qualification_list = []
            qualifications = qualification_values[c].split("|")
            for qualification in qualifications:
                qualification_list.append({"S" : qualification})
            multivalued_attribute = {"L":qualification_list}   
            expression_attribute_names["#PartnerCompetency"+str(c)] = qualification_names[c]
            expression_attribute_values[":partner_competency"+str(c)] = multivalued_attribute
            update_expression = update_expression + "#PartnerCompetency"+str(c)+" = :partner_competency"+str(c)+","           
    if partner_name != "" and partner_website != "":
        if len(update_expression) <= 4 :
            dynamodb.update_item(
                Key={
                    'partner_name': {
                        'S': partner_name.encode('utf-8'),
                    },
                    'partner_website': {
                        'S': partner_website,
                    },
                },
                ReturnValues = 'ALL_NEW',
                TableName = table_name
            )  
        else:
            dynamodb.update_item(
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                Key={
                    'partner_name': {
                        'S': partner_name.encode('utf-8'),
                    },
                    'partner_website': {
                        'S': partner_website,
                    },
                },
                ReturnValues='ALL_NEW',
                TableName=table_name,
                UpdateExpression=update_expression[:len(update_expression)-1]
            )      
browser.close()    