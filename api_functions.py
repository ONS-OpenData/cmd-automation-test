import requests
import json

def Get_Access_Token(credentials): # create inputs for email and password
    ### getting access_token ###
    '''
    credentials should be a path to file containing florence login email and password
    '''
    
    zebedee_url = 'https://publishing.develop.onsdigital.co.uk/zebedee/login'
    
    with open(credentials, 'r') as json_file:
        credentials_json = json.load(json_file)
    
    email = credentials_json['email']
    password = credentials_json['password']
    login = {"email":email, "password":password}
    
    r = requests.post(zebedee_url, json=login)
    if r.status_code == 200:
        access_token = r.text.strip('"')
        return access_token
    else:
        raise Exception('Token not created, returned a {} error'.format(r.status_code))


def Get_Recipe_Api(access_token):
    ''' returns whole recipe api '''
    
    recipe_api_url = 'https://publishing.develop.onsdigital.co.uk/recipes'
    headers = {'X-Florence-Token':access_token}
    
    r = requests.get(recipe_api_url, headers=headers)
    
    if r.status_code == 200:
        recipe_dict = r.json()
        return recipe_dict
    else:
        raise Exception('Recipe API returned a {} error'.format(r.status_code))
        
        
def Check_Recipe_Exists(access_token, dataset_id):
    '''
    Checks to make sure a recipe exists for dataset_id
    Returns nothing if recipe exists, an error if not
    Uses Get_Recipe_Api()
    '''
    recipe_dict = Get_Recipe_Api(access_token)
    # create a list of all existing dataset ids
    dataset_id_list = []
    for item in recipe_dict['items']:
        dataset_id_list.append(item['output_instances'][0]['dataset_id'])
    if dataset_id not in dataset_id_list:
        raise Exception('Recipe does not exist for {}'.format(dataset_id))
    

def Get_Recipe(access_token, dataset_id):
    ''' 
    Returns recipe for specific dataset 
    Uses Get_Recipe_Api()
    dataset_id is the dataset_id from the recipe
    '''
    Check_Recipe_Exists(access_token, dataset_id)
    recipe_dict = Get_Recipe_Api(access_token)
    # iterate through recipe api to find correct dataset_id
    for item in recipe_dict['items']:
        if dataset_id == item['output_instances'][0]['dataset_id']:
            return item


def Get_Recipe_Info(access_token, dataset_id):
    ''' 
    Returns useful recipe information for specific dataset 
    Uses Get_Recipe()
    '''
    recipe_dict = Get_Recipe(access_token, dataset_id)
    recipe_info_dict = {}
    recipe_info_dict['dataset_id'] = dataset_id
    recipe_info_dict['recipe_alias'] = recipe_dict['files'][0]['description']
    recipe_info_dict['recipe_id'] = recipe_dict['id']
    return recipe_info_dict
    
    
def Get_Recipe_Info_From_Recipe_Id(access_token, recipe_id):
    '''
    Returns useful recipe information for specific dataset
    Uses recipe_id to get recipe information
    '''
    
    recipe_api_url = 'https://publishing.develop.onsdigital.co.uk/recipes'
    single_recipe_url = recipe_api_url + '/' + recipe_id 
    
    headers = {'X-Florence-Token':access_token}
    
    r = requests.get(single_recipe_url, headers=headers)
    if r.status_code == 200:
        single_recipe_dict = r.json()
        return single_recipe_dict
    else:
        raise Exception('Get_Recipe_Info_From_Recipe_Id returned a {} error'.format(r.status_code))


def Get_Dataset_Instances_Api(access_token):
    ''' 
    Returns /dataset/instances API 
    '''
    dataset_instances_api_url = 'https://publishing.develop.onsdigital.co.uk/dataset/instances'
    headers = {'X-Florence-Token':access_token}
    
    r = requests.get(dataset_instances_api_url, headers=headers)
    if r.status_code == 200:
        dataset_instances_dict = r.json()
        return dataset_instances_dict
    else:
        raise Exception('/dataset/instances API returned a {} error'.format(r.status_code))


def Get_Latest_Dataset_Instances(access_token):
    '''
    Returns latest upload id
    Uses Get_Dataset_Instances_Api()
    '''
    dataset_instances_dict = Get_Dataset_Instances_Api(access_token)
    latest_id = dataset_instances_dict['items'][0]['id']
    return latest_id


def Get_Dataset_Instance_Info(access_token, instance_id):
    '''
    Return specific dataset instance info
    '''
    dataset_instances_url = 'https://publishing.develop.onsdigital.co.uk/dataset/instances/' + instance_id
    headers = {'X-Florence-Token':access_token}
    
    r = requests.get(dataset_instances_url, headers=headers)
    if r.status_code == 200:
        dataset_instances_dict = r.json()
        return dataset_instances_dict
    else:
        raise Exception('/dataset/instances/{} API returned a {} error'.format(instance_id, r.status_code))
    

def Get_Dataset_Jobs_Api(access_token):
    '''
    Returns dataset/jobs API
    '''

    dataset_jobs_api_url = 'https://publishing.develop.onsdigital.co.uk/dataset/jobs'
    headers = {'X-Florence-Token':access_token}

    r = requests.get(dataset_jobs_api_url + '?limit=1000', headers=headers)
    if r.status_code == 200:
        dataset_jobs_dict = r.json()
        return dataset_jobs_dict
    else:
        raise Exception('/dataset/jobs API returned a {} error'.format(r.status_code))
        
        
def Get_Latest_Job_Id(access_token):
    '''
    Returns latest job id and recipe id
    Uses Get_Dataset_Jobs_Api()
    '''
    dataset_jobs_dict = Get_Dataset_Jobs_Api(access_token)
    latest_id = dataset_jobs_dict['items'][-1]['id']
    recipe_id = dataset_jobs_dict['items'][-1]['recipe'] # to be used as a quick check
    return latest_id, recipe_id


def Create_New_Job(access_token, dataset_id, aws_link):
    '''
    Creates a new job in the /dataset/jobs API
    Job is created in state 'created'
    Uses Get_Recipe_Info() to get information
    '''
    dataset_dict = Get_Recipe_Info(access_token, dataset_id)
    
    dataset_jobs_api_url = 'https://publishing.develop.onsdigital.co.uk/dataset/jobs'
    headers = {'X-Florence-Token':access_token}
    
    new_job_json = {
        'recipe':dataset_dict['recipe_id'],
        'state':'created',
        'links':{},
        'files':[
            {
        'alias_name':dataset_dict['recipe_alias'],
        'url':aws_link
            }   
        ]
    }
        
    r = requests.post(dataset_jobs_api_url, headers=headers, json=new_job_json)
    if r.status_code == 201:
        print('Job created succefully')
    else:
        raise Exception('Job not created, return a {} error'.format(r.status_code))
        
    # return job ID
    job_id, job_recipe_id = Get_Latest_Job_Id(access_token)
    dataset_instance_id = Get_Latest_Dataset_Instances(access_token)
    # quick check to make sure newest job id is the correct one
    if job_recipe_id != dataset_dict['recipe_id']:
        print('New job recipe ID ({}) does not match recipe ID used to create new job ({})'.format(job_recipe_id, dataset_dict['recipe_id']))
    else:
        print('job_id - ', job_id)
        print('dataset_instance_id - ', dataset_instance_id)
        return job_id


def Add_File_To_Existing_Job(access_token, dataset_id, job_id, aws_link):
    '''
    Adds file to a job
    Only needed if file wasnt originally attached to new job - ie files = []
    '''

    dataset_dict = Get_Recipe_Info(access_token, dataset_id)
    
    attaching_file_to_job_url = 'https://publishing.develop.onsdigital.co.uk/dataset/jobs/' + job_id + '/files'
    headers = {'X-Florence-Token':access_token}
    
    added_file_json = {
            'alias_name':dataset_dict['recipe_alias'],
            'url':aws_link
            }

    r = requests.put(attaching_file_to_job_url, headers=headers, json=added_file_json)
    if r.status_code == 200:
        print('File added successfully')
    else:
        print('Error code {} from Add_File_To_Existing_Job'.format(r.status_code))


def Update_State_Of_Job(access_token, job_id):
    '''
    Updates state of job from created to submitted
    once submitted import process will begin
    '''

    updating_state_of_job_url = 'https://publishing.develop.onsdigital.co.uk/dataset/jobs/' + job_id
    headers = {'X-Florence-Token':access_token}

    updating_state_of_job_json = {}
    updating_state_of_job_json['state'] = 'submitted'
    
    # make sure file is in the job before continuing
    job_id_dict = Get_Job_Info(access_token, job_id)
    
    if len(job_id_dict['files']) != 0:
        r = requests.put(updating_state_of_job_url, headers=headers, json=updating_state_of_job_json)
        if r.status_code == 200:
            print('State updated successfully')
        else:
            print('State not updated, return error code {}'.format(r.status_code))
    else:
        raise Exception('Job does not have a v4 file!')
    

def Get_Job_Info(access_token, job_id):
    '''
    Return job info
    '''
    dataset_jobs_id_url = 'https://publishing.develop.onsdigital.co.uk/dataset/jobs/' + job_id
    headers = {'X-Florence-Token':access_token}
    
    r = requests.get(dataset_jobs_id_url, headers=headers)
    if r.status_code == 200:
        job_info_dict = r.json()
        return job_info_dict
    else:
        raise Exception('/dataset/jobs/{} returned error {}'.format(job_id, r.status_code))


def Upload_Data_To_Florence(credentials, dataset_id, aws_link):
    '''Uploads v4 to florence'''
    # get access_token
    access_token = Get_Access_Token(credentials)
    
    #quick check to make sure recipe exists in API
    Check_Recipe_Exists(access_token, dataset_id)
    
    # create new job
    job_id = Create_New_Job(access_token, dataset_id, aws_link)
    
    # update state of job
    Update_State_Of_Job(access_token, job_id)
