*** Settings ***
Documentation     This is a Test Suite to run all the CDIC tables one after the other
#Suite Setup       Suite Setup
#Suite Teardown    Suite Teardown
Library           ../Scripts/python/EtlValidationTestSteps.py
#Resource          ../Resources/Common_Cdic_Keywords.robot

*** Variables ***

*** Test Cases ***

#Command: robot -d Results Tests/EtlVallidation.robot

Run pipeline and get back the result
    Pipeline name   pl_test_wait
    Add Parameters for Duplicate Record Validation in ADLS
    #cpbdw  dimsubbranchs  pbss_dm  dev  rbpsw
    Get Bearer Token
    Trigger Specific Pipeline
    Get activity output     set test variable

*** Keywords ***

