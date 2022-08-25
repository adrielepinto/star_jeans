#!/usr/bin/env python
# coding: utf-8

# # Imports

# In[14]:


import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import numpy as np
import re
import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import logging


# In[15]:


# import os
# import requests
# from bs4 import BeautifulSoup
# from datetime import datetime
# import pandas as pd
# import numpy as np
# import re
# import sqlite3
# from sqlalchemy import create_engine
# import pandas as pd
# import logging

# ============================================= Parameters ========================================== #


# Get Url

#data Colection
def data_collection (url, headers):

    # # parameters 
    # url = 'https://www2.hm.com/en_us/men/products/jeans.html'
    # headers = {'user-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'}
    page = requests.get (url, headers = headers)

    # Api Requestes
    soup= BeautifulSoup (page.text, 'html.parser')


    # =================== Product Data ==================
    products = soup.find ('ul', class_ = 'products-listing small')
    product_list = products.find_all ('article', class_ = 'hm-product-item')

    #product id
    product_id = [p.get ('data-articlecode') for p in product_list ]

    #product category
    product_category= [p.get ('data-category') for p in product_list ]

    #product name
    product_list = products.find_all ('a', class_ = 'link')
    product_list [2].get_text ()

    product_name = [p.get_text () for p in product_list]

    #Product Price
    product_list = products.find_all ('span', class_ ='price regular')
    product_list [1].get_text()
    product_price = [p.get_text () for p in product_list]

    
    data = pd.DataFrame ([product_id, product_category, product_name, product_price]).T
    data.columns = ['product_id', 'product_category', 'product_name', 'product_price']

    #scrapy datetime
    datetime.now () .strftime ('%y-%m-%d %H:%M:%S')
    data ['scrapy_datetime'] = datetime.now () .strftime ('%y-%m-%d %H:%M:%S')
    
    return data





# In[16]:


data_collection


# # By Product

# In[17]:


def data_collection_by_product (data, headers):
    df1 = pd.DataFrame()

    for ic in data.product_id.tolist(): # Colors Collect
        url = 'https://www2.hm.com/en_us/productpage.'+ ic +'.html'
        logger.debug ('Product: %s', url)
        
        page = requests.get( url, headers=headers )
        soup = BeautifulSoup( page.text, 'html.parser' )

        # Color
        products_list = soup.find_all( 'a', class_='filter-option miniature' ) + soup.find_all( 'a', class_='filter-option miniature active' )
        p_colors = [p.get( 'data-color' ) for p in products_list]

        # Product Id
        p_articlecode = [p.get( 'data-articlecode' ) for p in products_list]

        df_color = pd.DataFrame( [p_articlecode, p_colors] ).T
        df_color.columns = ['Art. No.', 'color']

        for ip in range( len( df_color ) ): # Individual Product Colors Dataset
            url = 'https://www2.hm.com/en_us/productpage.' + df_color['Art. No.'][ip] + '.html'
            logger.debug ('Color: %s', url)
            
            page = requests.get( url, headers=headers )

            soup = BeautifulSoup( page.text, 'html.parser' ) # HTML With Soup
            p = [list(filter(None, x.get_text().split('\n'))) for x in soup.find_all('div','details-attributes-list-item')]
            price = [float(p.get_text().strip().replace('$', '')) for p in soup.find_all('span', 'price-value')]
            p = p+[['Price', price[0]]]
            df = pd.DataFrame( p ).T
            df.columns = df.iloc[0, :]
            df = df.iloc[1:, :]

            if not 'Care instructions' in df.columns.tolist():
                pass

            else:
                df = df.drop( columns=['Care instructions'], axis=1 )

                df = df.drop_duplicates()

                df1 = pd.concat( [df1, df], axis=0 )
                
    df2 = df1.copy() # Data Backup

    df2 = df2.drop_duplicates()

    df2 = df2.iloc[:, 2:]

    if 'Size' in df2.columns:
        df2 = df2.drop( columns=['Material', 'Imported', 'Concept', 'Nice to know', 'messages.clothingStyle', 'More sustainable materials', 'Size'], axis=1 )

    else:
        df2 = df2.drop( columns=['Material', 'Imported', 'Concept', 'Nice to know', 'messages.clothingStyle', 'More sustainable materials'], axis=1 )

    dfx = df2.iloc[3:, :]
    a = df2.iloc[:2, :]
    df2 = pd.concat( [dfx, a], axis=0 )

    df2.columns = ['fit', 'composition', 'color', 'product_id', 'price']

    df2 = df2.fillna( method='ffill' )

    df2 = df2[~df2['color'].str.contains('Solid-')]

    df2 = df2.reset_index( drop=True )

    df2.fit = [f.lower().replace(' ', '_') for f in df2.fit]
    df2.color = [f.lower().replace(' ', '_') for f in df2.color]

    for j in ['Pocket lining: ', 'Shell: ', 'Lining: ', 'Pocket: ']:
        df2.composition = [ic.strip() for ic in df2.composition.str.replace(j, '')]
        
        
    return df2


    data= data.drop (columns = ['product_id'], axis = 0 )
    


# In[18]:


data_collection_by_product


# # Data cleaning

# In[25]:



def data_cleaning (data_product):
    global data
    global df2
   

        
        # ===========================================
        


    df_ref = pd.DataFrame( index=range( len( data_product ) ), columns=['cotton_', 'polyester_', 'spandex_', 'elasterell_'] )

    df3 = data_product.composition.str.split(',', expand=True).reset_index(drop=True)

    df_cot0 = df3.loc[df3[0].str.contains('Cotton', na=True ), 0] # Need a For Loop on This.
    df_cot1 = df3.loc[df3[1].str.contains('Cotton', na=True ), 1]
    df_cot0.name, df_cot1.name = ['cotton', 'cotton']

    df_cott = df_cot0.combine_first( df_cot1 )
    df_ref = pd.concat( [df_ref, df_cott], axis=1 ).drop( columns=['cotton_'], axis=1 )

    df_poly0 = df3.loc[df3[0].str.contains('Polyester', na=True), 0]
    df_poly1 = df3.loc[df3[1].str.contains('Polyester', na=True), 1]
    df_poly0.name, df_poly1.name = ['polyester']*2

    df_poly = df_poly0.combine_first( df_poly1 )
    df_ref = pd.concat( [df_ref, df_poly], axis=1 ).drop( columns=['polyester_'], axis=1 )

    df_sp0 = df3.loc[df3[1].str.contains('Spandex', na=True), 1]
    df_sp1 = df3.loc[df3[2].str.contains('Spandex', na=True), 2]
    df_sp0.name, df_sp1.name = ['spandex']*2

    df_sp = df_sp0.combine_first( df_sp1 )
    df_ref = pd.concat( [df_ref, df_sp], axis=1 ).drop( columns=['spandex_'], axis=1 )

    # df_el = df3.loc[df3[1].str.contains('Elasterell', na=True), 1]
    # df_el.name = 'elasterell'

    # df_ref = pd.concat( [df_ref, df_el], axis=1 ).drop( columns=['elasterell_'], axis=1 )

    for f in df_ref.columns.tolist():
        df_ref[f] = df_ref[f].fillna(f.title() + ' 0%')
        df_ref[f] = df_ref[f].apply( lambda x: int(re.search('\d+', x).group(0))/100 )
        
    df4 = pd.concat( [data,data_product, df_ref], axis=1 )
    df4 = df4.drop( columns=['composition'], axis=1 )
    df4 = df4.reset_index( drop=True )
    df4 = df4.dropna()

    df4['datetime'] = datetime.now().strftime('%y-%m-%d %H:%M:%S')

    df4 = df4[['product_id','product_name','product_category','color','fit', 'price', 'cotton', 'polyester', 'spandex', 'datetime']] 
    data = (df4)
    return data


# In[26]:


data_product


# # Data Insert

# In[27]:


def data_insert (data):
    data_insert =data
    
    # Insert data
    conn = create_engine ('sqlite:///star_jeans_hm_db.sqlite', echo = False)
    
    #insert data to table
    data.to_sql ('vitrine', con =conn, if_exists = 'append', index = False)
    
    return None


# # Logging

# In[22]:


# if __name__ == '__main__':
    
#     # parameter in constance
#     headers = {'user-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'}
    
#     #url
#     url = 'https://www2.hm.com/en_us/men/products/jeans.html'
#     logger = logging.getLogger ('webscraping_hm')


#     # data Collection
#     data = data_collection (url, headers)
#     #logger.info ('data collect done')
    

#     # data colection by product
#     data_product = data_collection_by_product (data, headers)
#     #logger.info ('data collection by product done')
    
#     # data cleaning
#     data_propduct_cleaned = data_cleaning (data_product)
#     #logger.info ('data product cleaned done')           
                 
#     # data insertion
#     data_insert (data_propduct_cleaned)
#     #logger.info ('data insertion done')
        


# In[28]:


if __name__ == '__main__':
    
    path = '/Users/adriele/Documents/repos/python_ds_ao_dv/design_ETL'

    if not os.path.exists (path + 'Logs'):
        os.makedirs (path +'Logs')

    logging.basicConfig (
        filename = path + 'Logs/webscraping_hm.log',
        level = logging.DEBUG,
        format = '% (asctime)s - %(levelname)s - %(name)s - %(menssage)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        )
    logger = logging.getLogger ('webscraping_hm')

  

    # parameter in constance
    headers = {'user-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15'}
    
    #url
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'


    # data Collection
    data = data_collection (url, headers)
    logger.info ('data collect done')
    

    # data colection by product
    data_product = data_collection_by_product (data, headers)
    logger.info ('data collection by product done')
    
    # data cleaning
    data_propduct_cleaned = data_cleaning (data_product)
    logger.info ('data product cleaned done')           
                 
    # data insertion
    data_insert (data_propduct_cleaned)
    logger.info ('data insertion done')


# # mm

# In[35]:


* * * * * /Users/adriele/opt/anaconda3/lib/python3.9 /Users/adriele/Documents/repos/python_ds_ao_dv/design_ETLLogs/webscraping_hm.log


# In[36]:


crontab -l


# In[ ]:


ls -l


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




