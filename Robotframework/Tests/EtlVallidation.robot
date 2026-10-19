*** Settings ***
Documentation       This is a Test Suite to run all the CDIC tables one after the other
#Suite Setup        Suite Setup
#Suite Teardown     Suite Teardown
Library             ../Scripts/python/EtlValidationTestSteps.py
Library             SeleniumLibrary
#Resource           ../Resources/Common_Cdic_Keywords.robot

*** Variables ***

*** Test Cases ***

#Command: robot -d Results Tests/EtlVallidation.robot

#Run python pipeline and get back the result
#    Pipeline name   pl_test_adb_py_params
#    Add Parameters for Duplicate Record Validation in ADLS
#    #cpbdw  dimsubbranchs  pbss_dm  dev  rbpsw
#    Get Bearer Token
#    Trigger Specific Pipeline
#    Get activity output     Run Get final status Notebook
#
#Run pipeline and get back the result
#    Pipeline name   pl_test_adb_nb
#    Add Parameters for Duplicate Record Validation in ADLS
#    #cpbdw  dimsubbranchs  pbss_dm  dev  rbpsw
#    Get Bearer Token
#    Trigger Specific Pipeline
#    Get activity output     Run specified Notebook

ADB Sanity check
    open browser    about:blank     Chrome

*** Keywords ***

