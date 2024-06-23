import pandas as pd

test_cases = pd.read_excel("/Users/harish/PycharmProjects/april_automation_framework/config/Master_Test_Template.xlsx")

print(test_cases)

run_test_case = test_cases.loc[(test_cases.execution_ind == 'Y')]