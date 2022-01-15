{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "8f1eba82",
   "metadata": {
    "scrolled": true
   },
   "outputs": [],
   "source": [
    "import mysql.connector\n",
    "import csv\n",
    "import pymysql\n",
    "import sqlalchemy,pyodbc,os\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "bdc6b27d",
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "os.getcwd()\n",
    "os.chdir('D:/onlinework/assignment task/dataset')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "2c4b55c1",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['bristol-air-quality-data.csv']\n"
     ]
    }
   ],
   "source": [
    "print(os.listdir())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "fd075a3b",
   "metadata": {},
   "outputs": [],
   "source": [
    "import platform"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "cc332c6e",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "LAPTOP-KHTURMPM\n"
     ]
    }
   ],
   "source": [
    "print (platform.node())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "e923e248",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "'LAPTOP-KHTURMPM'"
      ]
     },
     "execution_count": 7,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import socket\n",
    "socket.gethostname()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "021e15b2",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['SQL Server',\n",
       " 'Oracle in OraDB18Home1',\n",
       " 'PostgreSQL ANSI(x64)',\n",
       " 'PostgreSQL Unicode(x64)',\n",
       " 'ODBC Driver 17 for SQL Server',\n",
       " 'Microsoft Access Driver (*.mdb, *.accdb)',\n",
       " 'Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)',\n",
       " 'Microsoft Access Text Driver (*.txt, *.csv)',\n",
       " 'MySQL ODBC 8.0 ANSI Driver',\n",
       " 'MySQL ODBC 8.0 Unicode Driver']"
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "pyodbc.drivers()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 33,
   "id": "bb8abe7a",
   "metadata": {},
   "outputs": [],
   "source": [
    "conn = pyodbc.connect(\"Driver={ODBC Driver 17 for SQL Server};\"\n",
    "    \"Server=(localdb)\\ProjectsV13;\"\n",
    "    \"Database=Database;\"\n",
    "    \"Trusted_Connection=yes;\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "2632582d",
   "metadata": {},
   "outputs": [],
   "source": [
    "mysql=\"CREATE TABLE [table1] ([Date Time] INT NOT NULL PRIMARY KEY, [NOx] NCHAR(10) NULL, [NO2] NCHAR(10) NULL, [NO] NCHAR(10) NULL, [SiteID] NCHAR(10) NULL, [PM10] NCHAR(10) NULL, [NVPM10] NCHAR(10) NULL, [VPM10] NCHAR(10) NULL, [NVPM2.5] NCHAR(10) NULL, [PM2.5] NCHAR(10) NULL, [VPM2.5] NCHAR(10) NULL, [CO] NCHAR(10) NULL, [O3] NCHAR(10) NULL,  [SO2] NCHAR(10) NULL, [Temperature] NCHAR(10) NULL, [RH] NCHAR(10) NULL, [Air Pressure] NCHAR(10) NULL, [Location] NCHAR(10) NULL, [geo_point_2d] NCHAR(10) NULL, [DateStart] NCHAR(10) NULL, [DateEnd] NCHAR(10) NULL,  [Current] NCHAR(10) NULL, [Instrument Type] NCHAR(10) NULL)\"\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "83e25e0c",
   "metadata": {},
   "outputs": [],
   "source": [
    "import pandas as pd\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "2024eda8",
   "metadata": {},
   "outputs": [],
   "source": [
    "df=pd.read_csv(\"D:/onlinework/assignment task/dataset/Clean.csv\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "ce895ef3",
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Unnamed: 0</th>\n",
       "      <th>Date Time</th>\n",
       "      <th>NOx</th>\n",
       "      <th>NO2</th>\n",
       "      <th>NO</th>\n",
       "      <th>SiteID</th>\n",
       "      <th>PM10</th>\n",
       "      <th>NVPM10</th>\n",
       "      <th>VPM10</th>\n",
       "      <th>NVPM2.5</th>\n",
       "      <th>...</th>\n",
       "      <th>SO2</th>\n",
       "      <th>Temperature</th>\n",
       "      <th>RH</th>\n",
       "      <th>Air Pressure</th>\n",
       "      <th>Location</th>\n",
       "      <th>geo_point_2d</th>\n",
       "      <th>DateStart</th>\n",
       "      <th>DateEnd</th>\n",
       "      <th>Current</th>\n",
       "      <th>Instrument Type</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>0</td>\n",
       "      <td>2013-08-23T07:00:00+00:00</td>\n",
       "      <td>51.54044</td>\n",
       "      <td>30.50055</td>\n",
       "      <td>13.72186</td>\n",
       "      <td>452.0</td>\n",
       "      <td>27.8</td>\n",
       "      <td>23.2</td>\n",
       "      <td>4.6</td>\n",
       "      <td>16.4</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>AURN St Pauls</td>\n",
       "      <td>51.4628294172,-2.58454081635</td>\n",
       "      <td>2006-06-15T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>1</td>\n",
       "      <td>2013-08-23T08:00:00+00:00</td>\n",
       "      <td>94.50000</td>\n",
       "      <td>44.25000</td>\n",
       "      <td>33.00000</td>\n",
       "      <td>203.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Brislington Depot</td>\n",
       "      <td>51.4417471802,-2.55995583224</td>\n",
       "      <td>2001-01-01T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>2</td>\n",
       "      <td>2013-08-23T10:00:00+00:00</td>\n",
       "      <td>242.75000</td>\n",
       "      <td>59.75000</td>\n",
       "      <td>119.50000</td>\n",
       "      <td>206.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Rupert Street</td>\n",
       "      <td>51.4554331987,-2.59626237324</td>\n",
       "      <td>2003-01-01T00:00:00+00:00</td>\n",
       "      <td>2015-12-31T00:00:00+00:00</td>\n",
       "      <td>False</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>3</td>\n",
       "      <td>2013-08-23T14:00:00+00:00</td>\n",
       "      <td>197.75000</td>\n",
       "      <td>73.25000</td>\n",
       "      <td>81.25000</td>\n",
       "      <td>270.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Wells Road</td>\n",
       "      <td>51.4278638883,-2.56374153315</td>\n",
       "      <td>2003-05-23T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>4</td>\n",
       "      <td>2013-08-23T18:00:00+00:00</td>\n",
       "      <td>81.00000</td>\n",
       "      <td>55.50000</td>\n",
       "      <td>16.50000</td>\n",
       "      <td>203.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Brislington Depot</td>\n",
       "      <td>51.4417471802,-2.55995583224</td>\n",
       "      <td>2001-01-01T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>...</th>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "      <td>...</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>18398</th>\n",
       "      <td>18400</td>\n",
       "      <td>2009-08-11T17:00:00+00:00</td>\n",
       "      <td>36.00000</td>\n",
       "      <td>29.00000</td>\n",
       "      <td>5.00000</td>\n",
       "      <td>452.0</td>\n",
       "      <td>14.0</td>\n",
       "      <td>10.0</td>\n",
       "      <td>4.0</td>\n",
       "      <td>7.0</td>\n",
       "      <td>...</td>\n",
       "      <td>3.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>AURN St Pauls</td>\n",
       "      <td>51.4628294172,-2.58454081635</td>\n",
       "      <td>2006-06-15T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>18399</th>\n",
       "      <td>18401</td>\n",
       "      <td>2009-08-11T19:00:00+00:00</td>\n",
       "      <td>162.00000</td>\n",
       "      <td>67.00000</td>\n",
       "      <td>63.00000</td>\n",
       "      <td>213.0</td>\n",
       "      <td>6.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Old Market</td>\n",
       "      <td>51.4560189999,-2.58348949026</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>False</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>18400</th>\n",
       "      <td>18402</td>\n",
       "      <td>2009-08-11T20:00:00+00:00</td>\n",
       "      <td>240.25000</td>\n",
       "      <td>91.25000</td>\n",
       "      <td>97.25000</td>\n",
       "      <td>206.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Rupert Street</td>\n",
       "      <td>51.4554331987,-2.59626237324</td>\n",
       "      <td>2003-01-01T00:00:00+00:00</td>\n",
       "      <td>2015-12-31T00:00:00+00:00</td>\n",
       "      <td>False</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>18401</th>\n",
       "      <td>18403</td>\n",
       "      <td>2009-08-11T21:00:00+00:00</td>\n",
       "      <td>190.00000</td>\n",
       "      <td>80.50000</td>\n",
       "      <td>71.50000</td>\n",
       "      <td>270.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Wells Road</td>\n",
       "      <td>51.4278638883,-2.56374153315</td>\n",
       "      <td>2003-05-23T00:00:00+00:00</td>\n",
       "      <td>NaN</td>\n",
       "      <td>True</td>\n",
       "      <td>Continuous (Reference)</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>18402</th>\n",
       "      <td>18404</td>\n",
       "      <td>2009-08-11T22:00:00+00:00</td>\n",
       "      <td>15.00000</td>\n",
       "      <td>13.00000</td>\n",
       "      <td>1.00000</td>\n",
       "      <td>452.0</td>\n",
       "      <td>13.0</td>\n",
       "      <td>11.0</td>\n",
       "      <td>2.0</td>\n",
       "      <td>11.0</td>\n",
       "      <td>...</td>\n",
       "      <td>3.0</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>AURN St Pauls</td>\n",
       "      <td>51.4628294172,-2.58454081635</td>\n",
       "      <td>2006-06-1</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "      <td>NaN</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>18403 rows × 24 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "       Unnamed: 0                  Date Time        NOx       NO2         NO  \\\n",
       "0               0  2013-08-23T07:00:00+00:00   51.54044  30.50055   13.72186   \n",
       "1               1  2013-08-23T08:00:00+00:00   94.50000  44.25000   33.00000   \n",
       "2               2  2013-08-23T10:00:00+00:00  242.75000  59.75000  119.50000   \n",
       "3               3  2013-08-23T14:00:00+00:00  197.75000  73.25000   81.25000   \n",
       "4               4  2013-08-23T18:00:00+00:00   81.00000  55.50000   16.50000   \n",
       "...           ...                        ...        ...       ...        ...   \n",
       "18398       18400  2009-08-11T17:00:00+00:00   36.00000  29.00000    5.00000   \n",
       "18399       18401  2009-08-11T19:00:00+00:00  162.00000  67.00000   63.00000   \n",
       "18400       18402  2009-08-11T20:00:00+00:00  240.25000  91.25000   97.25000   \n",
       "18401       18403  2009-08-11T21:00:00+00:00  190.00000  80.50000   71.50000   \n",
       "18402       18404  2009-08-11T22:00:00+00:00   15.00000  13.00000    1.00000   \n",
       "\n",
       "       SiteID  PM10  NVPM10  VPM10  NVPM2.5  ...  SO2  Temperature  RH  \\\n",
       "0       452.0  27.8    23.2    4.6     16.4  ...  NaN          NaN NaN   \n",
       "1       203.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "2       206.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "3       270.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "4       203.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "...       ...   ...     ...    ...      ...  ...  ...          ...  ..   \n",
       "18398   452.0  14.0    10.0    4.0      7.0  ...  3.0          NaN NaN   \n",
       "18399   213.0   6.0     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "18400   206.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "18401   270.0   NaN     NaN    NaN      NaN  ...  NaN          NaN NaN   \n",
       "18402   452.0  13.0    11.0    2.0     11.0  ...  3.0          NaN NaN   \n",
       "\n",
       "       Air Pressure           Location                  geo_point_2d  \\\n",
       "0               NaN      AURN St Pauls  51.4628294172,-2.58454081635   \n",
       "1               NaN  Brislington Depot  51.4417471802,-2.55995583224   \n",
       "2               NaN      Rupert Street  51.4554331987,-2.59626237324   \n",
       "3               NaN         Wells Road  51.4278638883,-2.56374153315   \n",
       "4               NaN  Brislington Depot  51.4417471802,-2.55995583224   \n",
       "...             ...                ...                           ...   \n",
       "18398           NaN      AURN St Pauls  51.4628294172,-2.58454081635   \n",
       "18399           NaN         Old Market  51.4560189999,-2.58348949026   \n",
       "18400           NaN      Rupert Street  51.4554331987,-2.59626237324   \n",
       "18401           NaN         Wells Road  51.4278638883,-2.56374153315   \n",
       "18402           NaN      AURN St Pauls  51.4628294172,-2.58454081635   \n",
       "\n",
       "                       DateStart                    DateEnd Current  \\\n",
       "0      2006-06-15T00:00:00+00:00                        NaN    True   \n",
       "1      2001-01-01T00:00:00+00:00                        NaN    True   \n",
       "2      2003-01-01T00:00:00+00:00  2015-12-31T00:00:00+00:00   False   \n",
       "3      2003-05-23T00:00:00+00:00                        NaN    True   \n",
       "4      2001-01-01T00:00:00+00:00                        NaN    True   \n",
       "...                          ...                        ...     ...   \n",
       "18398  2006-06-15T00:00:00+00:00                        NaN    True   \n",
       "18399                        NaN                        NaN   False   \n",
       "18400  2003-01-01T00:00:00+00:00  2015-12-31T00:00:00+00:00   False   \n",
       "18401  2003-05-23T00:00:00+00:00                        NaN    True   \n",
       "18402                  2006-06-1                        NaN     NaN   \n",
       "\n",
       "              Instrument Type  \n",
       "0      Continuous (Reference)  \n",
       "1      Continuous (Reference)  \n",
       "2      Continuous (Reference)  \n",
       "3      Continuous (Reference)  \n",
       "4      Continuous (Reference)  \n",
       "...                       ...  \n",
       "18398  Continuous (Reference)  \n",
       "18399  Continuous (Reference)  \n",
       "18400  Continuous (Reference)  \n",
       "18401  Continuous (Reference)  \n",
       "18402                     NaN  \n",
       "\n",
       "[18403 rows x 24 columns]"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "2a02ce3b",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sqlite3\n",
    "import sqlalchemy"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "1d8ec5b8",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\syed azmat ali abedi\\anaconda3\\lib\\site-packages\\pandas\\core\\generic.py:2779: UserWarning: The spaces in these column names will not be changed. In pandas versions < 0.14, spaces were converted to underscores.\n",
      "  sql.to_sql(\n"
     ]
    }
   ],
   "source": [
    "conn=sqlite3.connect('pollition_db')\n",
    "df.to_sql(\"pollution_db2\", con=conn, if_exists=\"append\", index=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "59cfb328",
   "metadata": {},
   "outputs": [],
   "source": [
    "#conn = sqlite3.connect('pollution-db2')\n",
    "c = conn.cursor()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "577766af",
   "metadata": {},
   "outputs": [],
   "source": [
    "#c.execute('CREATE TABLE IF NOT EXISTS products (product_name text, price number)')\n",
    "\n",
    "c.execute('CREATE TABLE IF NOT EXISTS pollution_db2 ([DateTime] INT NOT NULL PRIMARY KEY, [NOx] NCHAR(10) NULL, [NO2] NCHAR(10) NULL, [NO] NCHAR(10) NULL, [SiteID] NCHAR(10) NULL, [PM10] NCHAR(10) NULL, [NVPM10] NCHAR(10) NULL, [VPM10] NCHAR(10) NULL, [NVPM2.5] NCHAR(10) NULL, [PM2.5] NCHAR(10) NULL, [VPM2.5] NCHAR(10) NULL, [CO] NCHAR(10) NULL, [O3] NCHAR(10) NULL,  [SO2] NCHAR(10) NULL, [Temperature] NCHAR(10) NULL, [RH] NCHAR(10) NULL, [Air Pressure] NCHAR(10) NULL, [Location] NCHAR(10) NULL, [geo_point_2d] NCHAR(10) NULL, [DateStart] NCHAR(10) NULL, [DateEnd] NCHAR(10) NULL,  [Current] NCHAR(10) NULL, [Instrument Type] NCHAR(10) NULL)')\n",
    "conn.commit()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "cdca6f85",
   "metadata": {},
   "outputs": [],
   "source": [
    "sql = \"INSERT INTO  (name, address) VALUES (%s, %s)\"\n",
    "val = [\n",
    "  ('Peter', 'Lowstreet 4'),\n",
    "  ('Amy', 'Apple st 652'),\n",
    "  ('Hannah', 'Mountain 21'),\n",
    "  ('Michael', 'Valley 345'),\n",
    "  ('Sandy', 'Ocean blvd 2'),\n",
    "  ('Betty', 'Green Grass 1'),\n",
    "  ('Richard', 'Sky st 331'),\n",
    "  ('Susan', 'One way 98'),\n",
    "  ('Vicky', 'Yellow Garden 2'),\n",
    "  ('Ben', 'Park Lane 38'),\n",
    "  ('William', 'Central st 954'),\n",
    "  ('Chuck', 'Main Road 989'),\n",
    "  ('Viola', 'Sideway 1633')\n",
    "]\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "162db26f",
   "metadata": {},
   "outputs": [
    {
     "ename": "OperationalError",
     "evalue": "near \"(\": syntax error",
     "output_type": "error",
     "traceback": [
      "\u001b[1;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[1;31mOperationalError\u001b[0m                          Traceback (most recent call last)",
      "\u001b[1;32m<ipython-input-21-f44734928ce8>\u001b[0m in \u001b[0;36m<module>\u001b[1;34m\u001b[0m\n\u001b[1;32m----> 1\u001b[1;33m \u001b[0mc\u001b[0m\u001b[1;33m.\u001b[0m\u001b[0mexecutemany\u001b[0m\u001b[1;33m(\u001b[0m\u001b[0msql\u001b[0m\u001b[1;33m,\u001b[0m\u001b[0mval\u001b[0m\u001b[1;33m)\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[1;31mOperationalError\u001b[0m: near \"(\": syntax error"
     ]
    }
   ],
   "source": [
    "c.executemany(sql,val)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "16254e70",
   "metadata": {},
   "outputs": [],
   "source": [
    "with open ('insert-100.sql','w') as file:\n",
    "    file.write(sql)\n",
    "file.close()    "
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.8"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
