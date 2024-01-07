import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def recipe(data):
    recipe_list = []
    for data in data:
        recipe_id = data['id']
        recipe_name = data['recipe']
        category_id = data['category']['id']
        difficulty = data['difficulty']
        serving = data['serving']
        chef = data['chef']
        calories = data['calories']
        fat_in_grams = data['fat_in_grams']
        protein_in_grams = data['protein_in_grams']
        carbohydrates_in_grams = data['carbohydrates_in_grams']
        image_url = data['image']
        recipe_dict = {'recipe_id': recipe_id, 'recipe_name': recipe_name, 'category_id': category_id, 'difficulty': difficulty, 'serving': serving, 
                       'chef': chef, 'calories': calories, 'fat (gms)': fat_in_grams, 'carbohydrates (gms)': carbohydrates_in_grams, 
                       'protein (gms)': protein_in_grams, 'image_url': image_url}
        recipe_list.append(recipe_dict)
        recipe_df = pd.DataFrame.from_dict(recipe_list)
    recipe_df = recipe_df.drop_duplicates(recipe_df)
    return recipe_df

def category(data):
    category_list = []
    for data in data:
        category_id = data['category']['id']
        category = data['category']['category']
        thumbnail = data['category']['thumbnail']
        category_dict = {'category_id': category_id, 'category': category, 'thumbnail': thumbnail}
        category_list.append(category_dict)
        category_df = pd.DataFrame.from_dict(category_list)
    
    category_df =  category_df.drop_duplicates(category_df)
    return category_df
    
def ingredient(data):
    ingredient_list = []
    for data in data:
        recipe_id = data['id']
        recipe_name = data['recipe']
        prep_time_in_minutes = data['prep_time_in_minutes']
        cook_time_in_minutes = data['cook_time_in_minutes']
        ingredient_1 = data['ingredient_1']
        measurement_1 = data['measurement_1']
        ingredient_2 = data['ingredient_2']
        measurement_2 = data['measurement_2']
        ingredient_3 = data['ingredient_3']
        measurement_3 = data['measurement_3']
        ingredient_4 = data['ingredient_4']
        measurement_4 = data['measurement_4']
        ingredient_5 = data['ingredient_5']
        measurement_5 = data['measurement_5']
        ingredient_6 = data['ingredient_6']
        measurement_6 = data['measurement_6']
        ingredient_7 = data['ingredient_7']
        measurement_7 = data['measurement_7']
        ingredient_8 = data['ingredient_8']
        measurement_8 = data['measurement_8']
        ingredient_9 = data['ingredient_9']
        measurement_9 = data['measurement_9']
        ingredient_10 = data['ingredient_10']
        measurement_10 = data['measurement_10']
        ingredient_dict = {'recipe_id': recipe_id,'recipe_name': recipe_name, 'prep_time_in_minutes': prep_time_in_minutes, 'cook_time_in_minutes': cook_time_in_minutes, 'ingredient_1': ingredient_1,
                            'measurement_1': measurement_1, 'ingredient_2': ingredient_2, 'measurement_2': measurement_2,'ingredient_3': ingredient_3, 'measurement_3': measurement_3, 
                            'ingredient_4': ingredient_4, 'measurement_4': measurement_4, 'ingredient_5': ingredient_5, 'measurement_5': measurement_5,
                            'ingredient_6': ingredient_6, 'measurement_6': measurement_6, 'ingredient_7': ingredient_7, 'measurement_7': measurement_7, 'ingredient_8': ingredient_8,
                            'measurement_8': measurement_8, 'ingredient_9': ingredient_9,'measurement_9': measurement_9, 'ingredient_10': ingredient_10, 'measurement_10': measurement_10}
        ingredient_list.append(ingredient_dict)
        ingredient_df = pd.DataFrame.from_dict(ingredient_list)
    
    ingredient_df =  ingredient_df.drop_duplicates(ingredient_df)
    return ingredient_df

def direction(data):
    direction_list = []
    for data in data:
        recipe_id = data['id']
        recipe_name = data['recipe']
        directions_step_1 = data['directions_step_1']
        directions_step_2 = data['directions_step_2']
        directions_step_3 = data['directions_step_3']
        directions_step_4 = data['directions_step_4']
        directions_step_5 = data['directions_step_5']
        directions_step_6 = data['directions_step_6']
        directions_step_7 = data['directions_step_7']
        directions_step_8 = data['directions_step_8']
        directions_step_9 = data['directions_step_9']
        directions_step_10 = data['directions_step_10']
        direction_dict = {'recipe_id': recipe_id,'recipe_name': recipe_name, 'directions_step_1': directions_step_1, 'directions_step_2': directions_step_2,
                         'directions_step_3': directions_step_3, 'directions_step_4': directions_step_4, 'directions_step_5': directions_step_5,
                         'directions_step_6': directions_step_6, 'directions_step_7': directions_step_7, 'directions_step_8': directions_step_8,
                         'directions_step_9': directions_step_9, 'directions_step_10': directions_step_10}
        direction_list.append(direction_dict)
        direction_df = pd.DataFrame.from_dict(direction_list)
    
    direction_df =  direction_df.drop_duplicates(direction_df)
    return direction_df
    
def lambda_handler(event, context):
    s3 = boto3.client('s3')
    Bucket = 'keto-data-pipeline-raj'
    Key = 'raw_data/to_processed/'
    
    keto_data = []
    keto_keys = []
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            keto_data.append(jsonObject)
            keto_keys.append(file_key)
            
            
    for data in keto_data:
        recipe_df = recipe(data)
        category_df = category(data)
        ingredient_df = ingredient(data)
        direction_df = direction(data)
        
        recipe_key = 'transformed_data/recipe_data/recipe_transformed_' + str(datetime.now())+ '.csv'
        recipe_buffer = StringIO()
        recipe_df.to_csv(recipe_buffer, index= False)
        recipe_content = recipe_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = recipe_key, Body = recipe_content)
        
        category_key = 'transformed_data/category_data/category_transformed_' + str(datetime.now())+ '.csv'
        category_buffer = StringIO()
        category_df.to_csv(category_buffer, index= False)
        category_content = category_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = category_key, Body = category_content)
        
        ingredient_key = 'transformed_data/ingredient_data/ingredient_transformed_' + str(datetime.now())+ '.csv'
        ingredient_buffer = StringIO()
        ingredient_df.to_csv(ingredient_buffer, index= False)
        ingredient_content = ingredient_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = ingredient_key, Body = ingredient_content)
        
        direction_key = 'transformed_data/direction_data/direction_transformed_' + str(datetime.now())+ '.csv'
        direction_buffer = StringIO()
        direction_df.to_csv(direction_buffer, index= False)
        direction_content = direction_buffer.getvalue()
        s3.put_object(Bucket = Bucket, Key = direction_key, Body = direction_content)
        
    s3_resource = boto3.resource('s3')
    for key in keto_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
            }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/'+ key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
