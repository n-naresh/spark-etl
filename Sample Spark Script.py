from pyspark import SparkConf, SparkContext
import collections

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Import types
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql.functions import last_day
from pyspark.sql.window import *



def getSchemaClientID():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("CONVERSION_DT", DecimalType(7,0), False),
        ...
		...
		...
        StructField("MMIS_SEND_DT", DecimalType(9, 0), False)
    ])

    return schema

def getSchemaDemographic():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("BNFT_MO_BEG_DT", DecimalType(7, 0), False),
        StructField("SYS_POSTING_DT", DecimalType(9, 0), False),
        ...
		...
		...
        StructField("PREM_OK_IND", StringType(), False),
        StructField("PREM_OK_ENT_DT", DecimalType(9, 0), False)
    ])

    return schema

def getSchemaAliasName():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("ALIAS_LAST_NAME", StringType(), False),
        ...
		...
		...
        StructField("PROCESSOR_ID", StringType(), False)
    ])

    return schema

def getSchemaSsn():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("CL_SSN_NUM", DecimalType(9, 0), False),
        ...
		...
		...
        StructField("PROCESSOR_ID", StringType(), False)
    ])

    return schema

def getSchemaAlien():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("BNFT_MO_BEG_DT", DecimalType(7, 0), False),
        StructField("SYS_POSTING_DT", DecimalType(9, 0), False),
        ...
		...
		...
        StructField("BNFT_ST_PD_IND", StringType(), False)
    ])

    return schema

def getSchemaStudent():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("BNFT_MO_BEG_DT", DecimalType(7, 0), False),
        StructField("SYS_POSTING_DT", DecimalType(9, 0), False),
        ...
		...
		...
        StructField("GRAD_VER_DT", DecimalType(9, 0), False)
    ])

    return schema

def getSchemaEthnicity():

    # Specify schema
    schema = StructType([
        StructField("IRN", DecimalType(9,0), False),
        StructField("BNFT_MO_BEG_DT", DecimalType(7, 0), False),
        StructField("SYS_POSTING_DT", DecimalType(9, 0), False),
        ...
		...
		...
        StructField("PROCESSOR_ID", StringType(), False)
    ])

    return schema

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("spark_mdm_person") \
        .getOrCreate()

    inpFilePath = 'XXXX'
    outFilePath = 'XXXXXXX'

    inpClientID = "{}/TCLIENT_ID".format(inpFilePath)
    ClientIDSchema = getSchemaClientID()

    inpDemographic = "{}/TCL_DEMOGRAPHIC".format(inpFilePath)
    DemographicSchema = getSchemaDemographic()

    inpEthnicity = "{}/TCL_ETHNICITY".format(inpFilePath)
    EthnicitySchema = getSchemaEthnicity()

    inpSsn = "{}/TCL_SSN".format(inpFilePath)
    SsnSchema = getSchemaSsn()

    inpAlien = "{}/TCL_ALIEN".format(inpFilePath)
    AlienSchema = getSchemaAlien()

    inpStudent = "{}/TCL_STUDENT".format(inpFilePath)
    StudentSchema = getSchemaStudent()

    inpAliasName = "{}/TCL_ALIAS_NAME".format(inpFilePath)
    AliasNameSchema = getSchemaAliasName()

    # Read datasets
    try:

        ClientID = spark.read.csv(inpClientID, header='false', schema = ClientIDSchema, sep=',', quote ="'", escape='"')
        ClientID.createOrReplaceTempView("ClientID")

        Ethnicity = spark.read.csv(inpEthnicity, header='false', schema=EthnicitySchema, sep=',', quote ="'", escape='"')
        Ethnicity.createOrReplaceTempView("Ethnicity")

        Demographic = spark.read.csv(inpDemographic, header='false', schema=DemographicSchema, sep=',', quote ="'", escape='"')
        Demographic.createOrReplaceTempView("Demographic")

        Ssn = spark.read.csv(inpSsn, header='false', schema=SsnSchema, sep=',', quote ="'", escape='"')
        Ssn.createOrReplaceTempView("Ssn")

        Alien = spark.read.csv(inpAlien, header='false', schema=AlienSchema, sep=',', quote ="'", escape='"')
        Alien.createOrReplaceTempView("Alien")

        Student = spark.read.csv(inpStudent, header='false', schema=StudentSchema, sep=',', quote ="'", escape='"')
        Student.createOrReplaceTempView("Student")

        AliasName = spark.read.csv(inpAliasName, header='false', schema=AliasNameSchema, sep=',', quote ="'", escape='"')
        AliasName.createOrReplaceTempView("AliasName")


        try:
            # Process SSN Table
            df_Ssn = spark.sql("""
            select a.* 
            from Ssn a,
                 (select b.IRN, max(b.SYS_POSTING_DT) as max_dt
                   from Ssn b
                   where b.CL_SSN_TYPE_CD = 'P'
                    group by b.irn) c
            where a.irn = c.irn   
              and a.SYS_POSTING_DT = c.max_dt  
              and a.CL_SSN_TYPE_CD = 'P'  
            """)

            df_Ssn = df_Ssn.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(df_Ssn['SYS_POSTING_DT'].desc()))).select("*").filter("row_num = 1")

            df_Ssn.cache()

            df_Ssn.createOrReplaceTempView("Ssn_unique")

            # Process ETHINICITY Table
            df_Ethnicity = spark.sql("""
                                    select a.* 
                                    from Ethnicity a
                                    where a.BNFT_MO_END_DT = (select max(b.BNFT_MO_END_DT) from Ethnicity b
                                                                where b.irn  = a.irn)
                                    """)

            df_Ethnicity = df_Ethnicity.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(df_Ethnicity['BNFT_MO_BEG_DT'].desc()))).select("*").filter("row_num = 1")

            df_Ethnicity.cache()

            df_Ethnicity.createOrReplaceTempView("Ethnicity_latest")

            # Process ALIAS NAME Table
            df_alias_sorted = AliasName.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(AliasName['SYS_POSTING_DT'].desc()))).select("*").filter("row_num = 1")

            df_alias_sorted.cache()

            df_alias_sorted.createOrReplaceTempView("AliasName_sorted")

            # Process ALIEN Table
            df_Alien = spark.sql("""
                                    select a.*
                                    from Alien a
                                    where a.BNFT_MO_END_DT =   (select max(b.BNFT_MO_END_DT) from Alien b
                                                                where b.irn  = a.irn)
                                """)
            df_Alien = df_Alien.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(df_Alien['BNFT_MO_BEG_DT'].desc()))).select("*").filter("row_num = 1")

            df_Alien.cache()

            df_Alien.createOrReplaceTempView("Alien_latest")

            # Process Student Table
            df_student = spark.sql("""
                                    select a.*
                                    from Student a
                                    where a.BNFT_MO_END_DT =   (select max(b.BNFT_MO_END_DT) from Student b
                                                                where b.irn  = a.irn)
                                    """)

            df_student = df_student.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(df_student['BNFT_MO_BEG_DT'].desc()))).select("*").filter("row_num = 1")

            df_student.cache()

            df_student.createOrReplaceTempView("Student_latest")

            # Process Demographic Table
            df_Demographic = spark.sql("""
                                        select a.*
                                        from Demographic a
                                        where a.BNFT_MO_END_DT =   (select max(b.BNFT_MO_END_DT) from Demographic b
                                                                       where b.irn  = a.irn)
                                     """)

            df_Demographic = df_Demographic.withColumn("row_num", func.row_number().over(Window.partitionBy("IRN").orderBy(df_Demographic['BNFT_MO_BEG_DT'].desc()))).select("*").filter("row_num = 1")

            df_Demographic.cache()

            df_Demographic.createOrReplaceTempView("Demographic_latest")


            # Process Data from all Tables
            df = spark.sql("""
            
            select  current_timestamp() as last_update_date, 
			a.IRN as SOURCE_KEY, 
			trim(a.CL_FIRST_NAME) as first_name, trim(a.CL_MIDDLE_NAME) as mdl_name, trim(a.CL_LAST_NAME) as last_name, 
            b.CL_SSN_NUM as ssn, 
			...
			...
			...
            CONCAT(trim(e.ALIAS_FIRST_NAME), ' ', trim(e.ALIAS_MIDDLE_NAME), ' ', trim(e.ALIAS_LAST_NAME)) as alias_1, 
            ...
			...
			...
			CASE WHEN INTERPRETOR_IND = 'Y' THEN "N" ELSE "Y" END as eng_speak_ind,
            ...
			...
			...
            
            from ClientID a
            LEFT JOIN Ssn_unique b          ON a.irn = b.irn
            LEFT JOIN Demographic_latest c  ON a.irn = c.irn
            LEFT JOIN Ethnicity_latest d    ON a.irn = d.irn
            LEFT JOIN AliasName_sorted e    ON a.irn = e.irn
            LEFT JOIN Alien_latest f        ON a.irn = f.irn
            LEFT JOIN Student_latest g      ON a.irn = g.irn
            """)

            #df_sort = df.withColumn("row_num", func.row_number().over(Window.partitionBy().orderBy('SOURCE_KEY')))

            df = df.withColumn("row_num", func.monotonically_increasing_id())

            df.cache()

            df.createOrReplaceTempView("df_final")

            df_write_1 = spark.sql("""  
                            select row_num, 
							last_update_date, 
							SOURCE_KEY, 
							first_name, 
							...
							...
							...
							ma_id_suffix

                            from df_final
                            """)
            
            df_write = df_write_1.withColumn("suffix_cd", func.regexp_replace("suffix_cd", "[^A-Za-z]", "")) \
                                 .withColumn("prefix_cd", func.regexp_replace("prefix_cd", "[^A-Za-z]", ""))

        except Exception as e:
            print("Spark SQL Processing ... Failed", e)


        try:
            df_write.write.mode("overwrite").csv(outFilePath)
        except Exception as e:
            print("File Write...Failed", e)

        spark.sql('select count(*) from df_final').show()

    except Exception as e:
        print("File Load...Failed", e)