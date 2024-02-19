# Databricks notebook source
import sys

# COMMAND ----------

# Class1

class All_Env_Specific_Variables:


    def __init__(self, env, pod_name, dbfs_folder_base_path='/dbfs/FileStore/tables/DaaSWealth_QA/Main/'):

        # Variables that are common accress all environment
        self.dbfs_folder_base_path = dbfs_folder_base_path

        # Variables that are environment specific
        # srz specific variables

        az_cdic_db = 'home,srz/cdic,edaaaazepca00wm00gs0001,cdic'
        az_restrict_cdic_db = 'home,srz/cdic,edaaaazepca00wm00gs0001,cdic,srz'
        az_rahona_cdic_db = 'cdic,CDIC,edaaaaze1pca00wm0dwm0001,cdic'
        dev_cdic_db = 'cdic,CDIC,edaaaedle1devsrz,cdic'
        cz_staging_cdic_db = 'cdic,CDIC,edaaaedle1prdsrz,cdic'
        test_cdic_db = ',,,'

        az_cif_db = 'home,srz/cif,edaaaazepca00wm00gs0001,cif'
        az_restrict_cif_db = 'home,srz/cif,edaaaazepca00wm00gs0001,cif,srz'
        az_rahona_cif_db = 'cif,CIF,edaaaaze1pca00wm0dwm0001,cif'
        dev_cif_db = 'cif,CIF,edaaaedle1devsrz,cif'
        cz_staging_cif_db = 'cif,CIF,edaaaedle1prdsrz,cif'
        test_cif_db = ',,,'

        az_edw_db = 'home,srz/caedw,edaaaazepca00wm00gs0001,caedw'
        az_restrict_edw_db = 'home,srz/caedw,edaaaazepca00wm00gs0001,caedw,edw'
        az_rahona_edw_db = 'caedw,curated/caedw,edaaaecze1prdcz,caedw'
        dev_edw_db = 'caedw,CAEDW,edaaaedle1devsrz,caedw'
        # cz_staging_edw_db = 'edw,EDW,edaaaedle1prdsrz,edw'
        cz_staging_edw_db = 'caedw,curated/caedw,edaaaecze1prdcz,caedw'
        test_edw_db = ',,,'

        az_eoae_db = 'home,srz/eoae,edaaaazepca00wm00gs0001,eoae'
        az_restrict_eoae_db = 'home,srz/eoae,edaaaazepca00wm00gs0001,eoae,srz'
        az_rahona_eoae_db = 'eoae,curated/eoae,edaaaecze1prdcz,eoae'
        dev_eoae_db = 'eoae,EOAE,edaaaedle1devsrz,eoae'
        cz_staging_eoae_db = 'eoae,EOAE,edaaaedle1prdsrz,eoae'
        test_eoae_db = ',,,'

        az_occw_db = 'home,srz/occw,edaaaazepca00wm00gs0001,occw'
        az_restrict_occw_db = 'home,srz/occw,edaaaazepca00wm00gs0001,occw,srz'
        az_rahona_occw_db = 'occw,OCCW,edaaaaze1pca00wm0dwm0001,occw'
        dev_occw_db = 'occw,OCCW,edaaaedle1devsrz,occw'
        cz_staging_occw_db = 'occw,OCCW,edaaaedle1prdsrz,occw'
        test_occw_db = ',,,'

        az_odr_db = 'home,srz/odr,edaaaazepca00wm00gs0001,odr'
        az_restrict_odr_db = 'home,srz/odr,edaaaazepca00wm00gs0001,odr,srz'
        az_rahona_odr_db = 'odr,ODR,edaaaaze1pca00wm0dwm0001,odr'
        dev_odr_db = 'odr,ODR,edaaaedle1devsrz,odr'
        cz_staging_odr_db = 'odr,ODR,edaaaedle1prdsrz,odr'
        test_odr_db = ',,,'

        az_ossbr_db = 'home,srz/ossbr,edaaaazepca00wm00gs0001,ossbr'
        az_restrict_ossbr_db = 'home,srz/ossbr,edaaaazepca00wm00gs0001,ossbr,srz'
        az_rahona_ossbr_db = 'ossbr,OSSBR,edaaaaze1pca00wm0dwm0001,ossbr'
        dev_ossbr_db = 'ossbr,OSSBR,edaaaedle1devsrz,ossbr'
        cz_staging_ossbr_db = 'ossbr,OSSBR,edaaaedle1prdsrz,ossbr'
        test_ossbr_db = ',,,'

        az_ptsql_db = 'home,srz/ptsql,edaaaazepca00wm00gs0001,ptsql'
        az_restrict_ptsql_db = 'home,srz/ptsql,edaaaazepca00wm00gs0001,ptsql,srz'
        az_rahona_ptsql_db = 'ptsql,PTSQL,edaaaaze1pca00wm0dwm0001,ptsql'
        dev_ptsql_db = 'ptsql,PTSQL,edaaaedle1devsrz,ptsql'
        cz_staging_ptsql_db = 'ptsql,PTSQL,edaaaedle1prdsrz,ptsql'
        test_ptsql_db = ',,,'

        az_sfw_db = 'home,srz/sfw,edaaaazepca00wm00gs0001,sfw'
        az_restrict_sfw_db = 'home,srz/sfw,edaaaazepca00wm00gs0001,sfw,srz'
        az_rahona_sfw_db = 'sfw,SFW,edaaaaze1pca00wm0dwm0001,sfw'
        dev_sfw_db = 'sfw,SFW,edaaaedle1devsrz,sfw'
        cz_staging_sfw_db = 'sfw,SFW,edaaaedle1prdsrz,sfw'
        test_sfw_db = 'sfw,SFW,edaaaedle1prdsrz,sfw'

        az_upg_db = 'home,srz/upg,edaaaazepca00wm00gs0001,upg'
        az_restrict_upg_db = 'home,srz/upg,edaaaazepca00wm00gs0001,upg,srz'
        az_rahona_upg_db = 'upg,UPG,edaaaaze1pca00wm0dwm0001,upg'
        dev_upg_db = 'upg,UPG,edaaaedle1devsrz,upg'
        cz_staging_upg_db = 'upg,UPG,edaaaedle1prdsrz,upg'
        test_upg_db = ',,,'

        az_wca_db = 'home,srz/wca,edaaaazepca00wm00gs0001,wca'
        az_restrict_wca_db = 'home,srz/wca,edaaaazepca00wm00gs0001,wca,srz'
        az_rahona_wca_db = 'wca,WCA,edaaaaze1pca00wm0dwm0001,wca'
        dev_wca_db = 'wca,WCA,edaaaedle1devsrz,wca'
        cz_staging_wca_db = 'wca,WCA,edaaaedle1prdsrz,wca'
        test_wca_db = ',,,'

        az_wdlob_db = 'home,srz/wdlob,edaaaazepca00wm00gs0001,wdlob'
        az_restrict_wdlob_db = 'home,srz/wdlob,edaaaazepca00wm00gs0001,wdlob,srz'
        az_rahona_wdlob_db = 'wdlob,WDLOB,edaaaaze1pca00wm0dwm0001,wdlob'
        dev_wdlob_db = 'wdlob,WDLOB,edaaaedle1devsrz,wdlob'
        cz_staging_wdlob_db = 'wdlob,WDLOB,edaaaedle1prdsrz,wdlob'
        test_wdlob_db = 'wdlob,WDLOB,edaaaedle1prdsrz,wdlob'

        az_wdorg_db = 'home,srz/wdorg,edaaaazepca00wm00gs0001,wdorg'
        az_restrict_wdorg_db = 'home,srz/wdorg,edaaaazepca00wm00gs0001,wdorg,srz'
        az_rahona_wdorg_db = 'wdorg,WDORG,edaaaaze1pca00wm0dwm0001,wdorg'
        dev_wdorg_db = 'wdorg,WDORG,edaaaedle1devsrz,wdorg'
        cz_staging_wdorg_db = 'wdorg,WDORG,edaaaedle1prdsrz,wdorg'
        test_wdorg_db = ',,,'

        az_wdprm_db = 'home,srz/wdprm,edaaaazepca00wm00gs0001,wdprm'
        az_restrict_wdprm_db = 'home,srz/wdprm,edaaaazepca00wm00gs0001,wdprm,srz'
        az_rahona_wdprm_db = 'wdprm,WDPRM,edaaaaze1pca00wm0dwm0001,wdprm'
        dev_wdprm_db = 'wdprm,WDPRM,edaaaedle1devsrz,wdprm'
        cz_staging_wdprm_db = 'wdprm,WDPRM,edaaaedle1prdsrz,wdprm'
        test_wdprm_db = ',,,'

        az_wds_db = 'home,srz/wds,edaaaazepca00wm00gs0001,wds'
        az_restrict_wds_db = 'home,srz/wds,edaaaazepca00wm00gs0001,wds,srz'
        az_rahona_wds_db = 'wds,WDS,edaaaaze1pca00wm0dwm0001,wds'
        dev_wds_db = 'wds,WDS,edaaaedle1devsrz,wds'
        cz_staging_wds_db = 'wds,WDS,edaaaedle1prdsrz,wds'
        test_wds_db = ',,,'

        az_wdsm_db = 'home,srz/wdsm,edaaaazepca00wm00gs0001,wdsm'
        az_restrict_wdsm_db = 'home,srz/wdsm,edaaaazepca00wm00gs0001,wdsm,srz'
        az_rahona_wdsm_db = 'wdsm,WDSM,edaaaaze1pca00wm0dwm0001,wdsm'
        dev_wdsm_db = 'wdsm,WDSM,edaaaedle1devsrz,wdsm'
        cz_staging_wdsm_db = 'wdsm,WDSM,edaaaedle1prdsrz,wdsm'
        test_wdsm_db = ',,,'

        az_wfs_db = 'home,srz/wfs,edaaaazepca00wm00gs0001,wfs'
        az_restrict_wfs_db = 'home,srz/wfs,edaaaazepca00wm00gs0001,wfs,srz'
        az_rahona_wfs_db = 'wfs,WFS,edaaaaze1pca00wm0dwm0001,wfs'
        dev_wfs_db = 'wfs,WFS,edaaaedle1devsrz,wfs'
        cz_staging_wfs_db = 'wfs,WFS,edaaaedle1prdsrz,wfs'
        test_wfs_db = ',,,'

        az_wlp_db = 'home,srz/wlp,edaaaazepca00wm00gs0001,wlp'
        az_restrict_wlp_db = 'home,srz/wlp,edaaaazepca00wm00gs0001,wlp,srz'
        az_rahona_wlp_db = 'wlp,WLP,edaaaaze1pca00wm0dwm0001,wlp'
        dev_wlp_db = 'wlp,WLP,edaaaedle1devsrz,wlp'
        cz_staging_wlp_db = 'wlp,WLP,edaaaedle1prdsrz,wlp'
        test_wlp_db = ',,,'

        az_w360_db = 'home,srz/w360,edaaaazepca00wm00gs0001,w360'
        az_restrict_w360_db = 'home,srz/w360,edaaaazepca00wm00gs0001,w360,srz'
        az_rahona_w360_db = 'w360,W360,edaaaaze1pca00wm0dwm0001,w360'
        dev_w360_db = 'w360,W360,edaaaedle1devsrz,w360'
        cz_staging_w360_db = 'w360,W360,edaaaedle1prdsrz,w360'
        test_w360_db = ',,,'

        # cz specific variables

        az_pbcontrib_db = 'home,cz/cpbdw/rbpsw/pbcontrib_dbo,edaaaazepca00wm00gs0001,unknown'
        az_restrict_pbcontrib_db = 'home,cz/cpbdw/rbpsw/pbcontrib_dbo,edaaaazepca00wm00gs0001,unknown,srz'
        az_rahona_pbcontrib_db = 'tczcpbdw,cpbdw/rbpsw/pbcontrib_dbo,edaaaaze1pca00wm0dwm0001,unknown'
        dev_pbcontrib_db = 'tczcpbdw,cpbdw/rbpsw/pbcontrib_dbo,edaaatcze1devcz,unknown'
        cz_staging_pbcontrib_db = 'tczcpbdw,cpbdw/rbpsw/pbcontrib_dbo,edaaatcze1prdcz,unknown'
        test_pbcontrib_db = ',,,'

        az_tcz04_db = 'home,curated/bbcmn/out,edaaaazepca00wm00gs0001,unknown'
        az_restrict_tcz04_db = 'home,curated/bbcmn/out,edaaaazepca00wm00gs0001,unknown,srz'
        az_rahona_tcz04_db = 'tcz0004,curated/bbcmn/out,edaaaaze1pca00wm0dwm0001,unknown'
        dev_tcz04_db = 'tcz0004,curated/bbcmn/out,edaaatcze1devcz,unknown'
        cz_staging_tcz04_db = 'tcz0004,curated/bbcmn/out,edaaatcze1prdcz,unknown'
        test_tcz04_db = ',,,'

        az_rahona_cz_db = 'unknown,UNKNOWN,unknown,unknown'

        az_cz01_db = 'home,curated/wcz0001,edaaaazepca00wm00gs0001,na'
        az_restrict_cz01_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz01'
        dev_cz01_db = 'wcz0001,curated/wcz0001,edaaawcze1devcz,na'
        cz_staging_cz01_db = 'wcz0001,curated/wcz0001,edaaawcze1prdcz,na'
        test_cz01_db = ',,,'

        az_cz01stg_db = 'home,curated/wcz0001,edaaaazepca00wm00gs0001,na'
        az_restrict_cz01stg_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz01'
        dev_cz01stg_db = 'wcz0001,curated/wcz0001,edaaawcze1stgcz,na'
        cz_staging_cz01stg_db = 'wcz0001,curated/wcz0001,edaaawcze1stgcz,na'
        test_cz01stg_db = ',,,'

        az_cz02_db = 'home,curated/wcz0002,edaaaazepca00wm00gs0001,na'
        az_restrict_cz02_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz02'
        dev_cz02_db = 'wcz0002,curated/wcz0002,edaaawcze1devcz,na'
        cz_staging_cz02_db = 'wcz0002,curated/wcz0002,edaaawcze1prdcz,na'
        test_cz02_db = ',,,'

        az_cz03_db = 'home,curated/wcz0003,edaaaazepca00wm00gs0001,na'
        az_restrict_cz03_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz03'
        dev_cz03_db = 'wcz0003,curated/wcz0003,edaaawcze1devcz,na'
        cz_staging_cz03_db = 'wcz0003,curated/wcz0003,edaaawcze1prdcz,na'
        test_cz03_db = ',,,'

        az_cz03stg_db = 'home,curated/wcz0003,edaaaazepca00wm00gs0001,na'
        az_restrict_cz03stg_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz03'
        dev_cz03stg_db = 'wcz0001,curated/wcz0003,edaaawcze1stgcz,na'
        cz_staging_cz03stg_db = 'wcz0003,curated/wcz0003,edaaawcze1stgcz,na'
        test_cz03stg_db = ',,,'

        az_cz04_db = 'home,curated/wcz0004,edaaaazepca00wm00gs0001,na'
        az_restrict_cz04_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz04'
        dev_cz04_db = 'wcz0004,curated/wcz0004,edaaawcze1devcz,na'
        cz_staging_cz04_db = 'wcz0004,curated/wcz0004,edaaawcze1prdcz,na'
        test_cz04_db = ',,,'

        az_cz05_db = 'home,curated/wcz0005,edaaaazepca00wm00gs0001,na'
        az_restrict_cz05_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz05'
        dev_cz05_db = 'wcz0005,curated/wcz0005,edaaawcze1devcz,na'
        cz_staging_cz05_db = 'wcz0005,curated/wcz0005,edaaawcze1prdcz,na'
        test_cz05_db = ',,,'

        az_cz06_db = 'home,curated/wcz0006,edaaaazepca00wm00gs0001,na'
        az_restrict_cz06_db = 'unknown,UNKNOWN,unknown,wealth_data_warehouse,cz06'
        dev_cz06_db = 'wcz0006,curated/wcz0006,edaaawcze1devcz,na'
        cz_staging_cz06_db = 'wcz0006,curated/wcz0006,edaaawcze1prdcz,na'
        test_cz06_db = ',,,'

        # cz_base_path

        az_cz_base_path = {'dc': 'abfss://home@edaaaazepca00wm00gs0001.dfs.core.windows.net/curated/wcz0001',
                           'ep1': 'abfss://home@edaaaazepca00wm00gs0001.dfs.core.windows.net/curated/wcz0006',
                           'ep2': 'abfss://home@edaaaazepca00wm00gs0001.dfs.core.windows.net/curated/wcz0006'}
        az_restrict_cz_base_path = {'dc': 'abfs://home@edaaaazepca00wm0rao0001.dfs.core.windows.net/data/delta_lake',
                                    'ep1': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006_test',
                                    'ep2': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006_test'}
        az_rahona_cz_base_path = {'dc': 'abfss://home@edaaaaze1pca00wm0dwm0001.dfs.core.windows.net/curated/wcz0001/dev_lob',
                                    'ep1': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006_test',
                                    'ep2': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006_test'}
        dev_cz_base_path = {'dc': 'abfss://wcz0001@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0001',
                            'ep1': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006',
                            'ep2': 'abfss://wcz0006@edaaawcze1devcz.dfs.core.windows.net/curated/wcz0006'}
        cz_staging_cz_base_path = {'dc': 'abfss://wcz0003@edaaawcze1stgcz.dfs.core.windows.net/curated/wcz0003',
                                   'ep1': 'abfss://wcz0006@edaaawcze1stgcz.dfs.core.windows.net/curated/wcz0006',
                                   'ep2': 'abfss://wcz0006@edaaawcze1stgcz.dfs.core.windows.net/curated/wcz0006'}
        test_cz_base_path = {'dc': 'abfss://processed@karadlsdev.dfs.core.windows.net',
                               'ep1': 'abfss://processed@karadlsdev.dfs.core.windows.net',
                               'ep2': 'abfss://processed@karadlsdev.dfs.core.windows.net'}

        # cz_mount_path

        az_cz_mount_path = {'dc': 'mnt/home/curated/wcz0001',
                           'ep1': 'mnt/home/curated/wcz0006',
                           'ep2': 'mnt/home/curated/wcz0006'}
        az_restrict_cz_mount_path = {'dc': 'mnt/home/data/delta_lake',
                                    'ep1': 'mnt/home/data/delta_lake',
                                    'ep2': 'mnt/home/data/delta_lake'}
        az_rahona_cz_mount_path = {'dc': 'mnt/home/curated/wcz0001/dev_lob',
                                    'ep1': 'mnt/home/curated/wcz0006_test',
                                    'ep2': 'mnt/home/curated/wcz0006_test'}
        dev_cz_mount_path = {'dc': 'mnt/wcz0001/curated/wcz0001',
                            'ep1': 'mnt/wcz0006/curated/wcz0006',
                            'ep2': 'mnt/wcz0006/curated/wcz0006'}
        cz_staging_cz_mount_path = {'dc': 'mnt/wcz0003/curated/wcz0003',
                                   'ep1': 'mnt/wcz0006/curated/wcz0006',
                                   'ep2': 'mnt/wcz0006/curated/wcz0006'}
        test_cz_mount_path = {'dc': 'mnt/processed',
                             'ep1': 'mnt/processed',
                             'ep2': 'mnt/processed'}
        # synapse specific variables
        # synapse srz variables

        az_srz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        az_restrict_srz_synapse_conn = {'server':'p3001-eastus2-asql-3.database.windows.net',
                                        'database':'eda-akora2-aaedl-srzpoolprd','scope':'aaaz-base',
                                        'username':'SP_ADB_AAAZ_ca00wm0rao0001_PRD_AppID',
                                        'password':'SP_ADB_AAAZ_ca00wm0rao0001_PRD_PWD'}
        az_rahona_srz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        dev_srz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        cz_staging_srz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        test_srz_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}

        az_edw_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        az_restrict_edw_synapse_conn = {'server': 'p3001-eastus2-asql-2.database.windows.net',
                                        'database': 'eda-akora2-aaecz-corporatepoolprd', 'scope': 'aaaz-base',
                                        'username': 'SP_ADB_AAAZ_ca00wm0rao0001_PRD_AppID',
                                        'password': 'SP_ADB_AAAZ_ca00wm0rao0001_PRD_PWD'}
        az_rahona_edw_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        dev_edw_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        cz_staging_edw_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        test_edw_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}

        # synapse cz variables

        az_cz01_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        az_restrict_cz01_synapse_conn = {'server': 'p3004-eastus2-asql-42.database.windows.net',
                                        'database': 'eda-akora-aawcz-wcz0001poolprd', 'scope': 'aaaz-base',
                                        'username': 'SP_ADB_AAAZ_ca00wm0rao0001_PRD_AppID',
                                        'password': 'SP_ADB_AAAZ_ca00wm0rao0001_PRD_PWD'}
        az_rahona_cz01_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        dev_cz01_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        cz_staging_cz01_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}
        test_cz01_synapse_conn = {'server': '', 'database': '', 'scope': '', 'username': '', 'password': ''}

        # synapse main(old) variables

        az_main_synapse_conn = {'dc':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'},
                            'ep1':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'},
                            'ep2':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'}}
        az_restrict_main_synapse_conn = {'dc':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                        'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'},
                                    'ep1':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                        'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'},
                                    'ep2':{'server':'p3002-eastus2-asql-133.database.windows.net','database':'eda-akora-aaaz-CA00WM00GS0001poolprd','scope':'aaaz-base',
                                        'username':'SP_ADB_AAAZ_CA00WM00GS0001_PRD-AppID','password':'SP_ADB_AAAZ_CA00WM00GS0001_PRD_PWD'}}
        az_rahona_main_synapse_conn = {}
        dev_main_synapse_conn = {'dc':{'server':'d3004-eastus2-asql-42.database.windows.net','database':'eda-akora-aawcz-curatedpooldev','scope':'aawcz',
                                'username':'SP_ADB_AAWCZ_CuratedZone_DEV-AppID','password':'SP_ADB_AAWCZ_CuratedZone_DEV-PWD'},
                            'ep1':{'server':'d3004-eastus2-asql-42.database.windows.net','database':'eda-akora-aawcz-wcz0005pooldev','scope':'aawcz',
                                'username':'SP_ADB_AAWCZ_WCZ0005_DEV-AppID','password':'SP_ADB_AAWCZ_WCZ0005_DEV-PWD'},
                            'ep2':{'server':'d3004-eastus2-asql-42.database.windows.net','database':'eda-akora-aawcz-wcz0005pooldev','scope':'aawcz',
                                'username':'SP_ADB_AAWCZ_WCZ0005_DEV-AppID','password':'SP_ADB_AAWCZ_WCZ0005_DEV-PWD'}}
        cz_staging_main_synapse_conn = {}
        test_main_synapse_conn = {}

        # Variables that change accross different env
        exec(f"self.cdic_db={env}_cdic_db.split(',')")
        exec(f"self.cif_db={env}_cif_db.split(',')")
        exec(f"self.edw_db={env}_edw_db.split(',')")
        exec(f"self.eoae_db={env}_eoae_db.split(',')")
        exec(f"self.occw_db={env}_occw_db.split(',')")
        exec(f"self.odr_db={env}_odr_db.split(',')")
        exec(f"self.ossbr_db={env}_ossbr_db.split(',')")
        exec(f"self.ptsql_db={env}_ptsql_db.split(',')")
        exec(f"self.sfw_db={env}_sfw_db.split(',')")
        exec(f"self.upg_db={env}_upg_db.split(',')")
        exec(f"self.wca_db={env}_wca_db.split(',')")
        exec(f"self.wdlob_db={env}_wdlob_db.split(',')")
        exec(f"self.wdorg_db={env}_wdorg_db.split(',')")
        exec(f"self.wdprm_db={env}_wdprm_db.split(',')")
        exec(f"self.wds_db={env}_wds_db.split(',')")
        exec(f"self.wdsm_db={env}_wdsm_db.split(',')")
        exec(f"self.wfs_db={env}_wfs_db.split(',')")
        exec(f"self.wlp_db={env}_wlp_db.split(',')")
        exec(f"self.w360_db={env}_w360_db.split(',')")
        exec(f"self.pbcontrib_db={env}_pbcontrib_db.split(',')")
        exec(f"self.tcz04_db={env}_tcz04_db.split(',')")
        if env == 'az_rahona':
            self.cz01_db = az_rahona_cz_db.split(',')
            self.cz01stg_db = self.cz01_db
            self.cz02_db = self.cz01_db
            self.cz03_db = self.cz01_db
            self.cz03stg_db = self.cz01_db
            self.cz04_db = self.cz01_db
            self.cz05_db = self.cz01_db
            self.cz06_db = self.cz01_db
        else:
            exec(f"self.cz01_db={env}_cz01_db.split(',')")
            exec(f"self.cz01stg_db={env}_cz01stg_db.split(',')")
            exec(f"self.cz02_db={env}_cz02_db.split(',')")
            exec(f"self.cz03_db={env}_cz03_db.split(',')")
            exec(f"self.cz03stg_db={env}_cz03stg_db.split(',')")
            exec(f"self.cz04_db={env}_cz04_db.split(',')")
            exec(f"self.cz05_db={env}_cz05_db.split(',')")
            exec(f"self.cz06_db={env}_cz06_db.split(',')")

        exec(f"self.srz_synapse_conn={env}_srz_synapse_conn")
        exec(f"self.edw_synapse_conn={env}_edw_synapse_conn")
        exec(f"self.cz01_synapse_conn={env}_cz01_synapse_conn")
        # Variables that change accross different env and pod_name
        exec(f"self.main_synapse_conn_dict={env}_main_synapse_conn")
        exec(f"self.cz_base_path_dict={env}_cz_base_path")
        exec(f"self.cz_mount_path_dict={env}_cz_mount_path")

        self.main_synapse_conn = self.main_synapse_conn_dict.get(pod_name,'')
        self.cz_base_path = self.cz_base_path_dict.get(pod_name,'')
        self.cz_mount_path = self.cz_mount_path_dict.get(pod_name,'')

        self.db_names = {'cdic':self.cdic_db, 'cif':self.cif_db, 'edw':self.edw_db, 'eoae':self.eoae_db,
                        'occw':self.occw_db, 'odr':self.odr_db, 'ossbr':self.ossbr_db, 'ptsql':self.ptsql_db,
                        'sfw':self.sfw_db,'upg':self.upg_db, 'wca':self.wca_db, 'wdlob':self.wdlob_db,
                        'wdorg':self.wdorg_db, 'wdprm':self.wdprm_db, 'wds':self.wds_db, 'wdsm':self.wdsm_db,
                        'wfs':self.wfs_db, 'wlp':self.wlp_db, 'w360':self.w360_db, 'pbcontrib':self.pbcontrib_db,
                        'tcz04':self.tcz04_db, 'cz01':self.cz01_db, 'cz01stg':self.cz01stg_db, 'cz02': self.cz02_db,
                        'cz03':self.cz03_db, 'cz03stg':self.cz03stg_db, 'cz04':self.cz04_db, 'cz05':self.cz05_db,
                        'cz06': self.cz06_db}
        self.synapse_conn = {'main_synapse_conn':self.main_synapse_conn, 'srz_synapse_conn':self.srz_synapse_conn,
                             'edw_synapse_conn':self.edw_synapse_conn, 'cz01_synapse_conn':self.cz01_synapse_conn}
        print(f'db_names : {self.db_names}\nmain_synapse_conn: {self.main_synapse_conn}')
        print(f'cz_base_path: {self.cz_base_path}\ncz_mount_path: {self.cz_mount_path}')


    def __str__(self):
        return """This class stores all the variables that change accross environment, based on env and pod_name.
            All the arguments passes to the class during declaration:
            env
            pod_name"""

    
    def common_vars_add_to_config(self, config_file_path):

        sys.path.insert(1, f'{self.dbfs_folder_base_path}/Scripts/python'.replace('//','/'))
        import dates_needed

        # self.comman_variables()

        # Adding DB variables
        # srorageAcc_container_relation = {'edaaaedle1devsrz':['cdic', 'cif', 'ntd', 'edw', 'odr', 'ossbr', 'pic', 'ptsql', 'rio', 'sfw', 'tcom', 'upg', 'wca', 'wdar', 'wdlob', 'wdorg', 'wdorg', 'wdprm', 'wds', 'wdsm', 'wlp'], 'edaaawcze1devcz':['wcz0001', 'wcz0003', 'wcz0006']}   
        # print(type(self.db_names))
        malcode_dict = dict()
        for db_key, db_value in self.db_names.items():
            #print(db_key, db_value)
            
            malcode_dict[f'container_{db_key}'] = db_value[0]
            malcode_dict[f'folder_path_{db_key}'] = db_value[1]
            malcode_dict[f'storage_acc_{db_key}'] = db_value[2]
            malcode_dict[f'database_{db_key}'] = db_value[3]
            if len(db_value) == 5:
                malcode_dict[f'synapse_conn_{db_key}'] = db_value[4]

        conf = dates_needed.add_aditional_parameters(config_file_path, dbfs_folder_base_path=self.dbfs_folder_base_path,
                                                     **malcode_dict, **self.synapse_conn, cz_base_path=self.cz_base_path,
                                                     cz_mount_path=self.cz_mount_path)

# COMMAND ----------

# # # Uncomment if you want to run this file alone, for testing
# if __name__ == '__main__':
#     env = 'az_restrict'
#     pod_name = 'ep1'
#     config_file_path = 'C:/Users/narayk5/OneDrive - The Toronto-Dominion Bank/Documents/GitHub4/daaswealth_qa_cloud/Scripts_v2/Config/config_file_kar.yaml'
#     dbfs_folder_base_path = 'C:/Users/narayk5/OneDrive - The Toronto-Dominion Bank/Documents/GitHub4/daaswealth_qa_cloud/Scripts_v2'
#     cmn_vars = All_Env_Specific_Variables(env, pod_name, dbfs_folder_base_path)
#     cmn_vars.common_vars_add_to_config(config_file_path)
