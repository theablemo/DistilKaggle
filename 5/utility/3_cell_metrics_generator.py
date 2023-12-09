### NOTE: `2_aggregated_dataframe_generator.py` should be ran prior to running this file

import pandas as pd
import re
import os
import statistics as sts
from radon.visitors import ComplexityVisitor
import collections
from tqdm import tqdm
tqdm.pandas()

# Set paths
dataframes_folder_path = '../dataframes' # Dataframes path
code_dataframe = f'{dataframes_folder_path}/code.csv'
markdown_dataframe = f'{dataframes_folder_path}/markdown.csv'
metrics_folder_path = '../metrics'
code_cell_metrics_dataframe = f'{metrics_folder_path}/code_cell_metrics.csv'
markdown_cell_metrics_dataframe = f'{metrics_folder_path}/markdown_cell_metrics.csv'

# Check if metrics directory exists and create if not
if not os.path.exists(metrics_folder_path):
    os.makedirs(metrics_folder_path)

# Get dataframes
Code_df = pd.read_csv(code_dataframe)
Code_df.fillna('', inplace=True)
md_df = pd.read_csv(markdown_dataframe)
md_df.fillna('', inplace=True)

############## CODE METRICS ##############

# Helper Methods for Code Metrics

# Extract number of python operators from text
def extract_arithmatic_operator_count(text):
    operators = [
        '+', '-', '*', '/', '**', '%', '//', '^',
        '=', '+=', '-=', '*=', '/=', '%=', '//=', '**=',
        '&=', '|=', '^=', '>>=', '<<=', '<<', '>>'
    ]
    count = 0
    for operator in operators:
        count += text.count(operator)
    return count

def extract_comparision_operator_count(text):
    operators = [
        '==', '!=', '>=', '<=', '>', '<'
    ]
    count = 0
    for operator in operators:
        count += text.count(operator)
    return count

def boolean_logic_operator_count(text):
    operators = [
        ' and ', ' or ', ' not ', '&', '|',
        ' is ', ' is not ',
        ' in ', ' not in ', '~'

    ]
    count = 0
    for operator in operators:
        count += text.count(operator)
    return count

def extract_unique_operator_count(text):
    operators = [
        '+', '-', '*', '/', '**', '%', '//',
        '=', '+=', '-=', '*=', '/=', '%=', '//=', '**=',
        '&=', '|=', '^=', '>>=', '<<=',
        '==', '!=', '>=', '<=', '>', '<',
        ' and ', ' or ', ' not ',
        ' is ', ' is not ',
        ' in ', ' not in ',
        '&', '|', '^', '~', '<<', '>>'

    ]
    unique_count = 0
    for operator in operators:
        if text.count(operator) > 0:
            unique_count += 1
    return unique_count

# Remove file address from string
def remove_file_address(text): 
    text = re.sub(r'\S*[\\|/]\S*', '', text)
    return text
def remove_ampersand(text):
    text = re.sub(r'%[a-zA-Z]+', '', text)
    return text
def remove_extras(text):
    text = remove_ampersand(text)
    text = remove_file_address(text)
    return text

# Extract python keywords from text
def extract_keywords_count(text):
    keywords = ['True', 'False', 'None', 'and', 'as', 'assert', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield']
    count = 0
    for keywords in keywords:
        count += text.count(keywords)
    return count

# Count the number of identifiers in the given text, excluding Python keywords
keywords = ['True', 'False', 'None', 'and', 'as', 'assert', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield']
def extract_identifier_count(text):
    count = 0
    identifiers = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', text)
    for identifier in identifiers:
        if identifier not in keywords:
            count += 1
    return count

# Calculate the average length of identifiers in the given text, excluding Python keywords
def extract_avg_len_identifier(text):
    iden_lens = []
    identifiers = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', text)
    for identifier in identifiers:
        if identifier not in keywords:
            iden_lens.append(len(identifier))
    if len(iden_lens) == 0:
        return 0
    return sum(iden_lens) // len (iden_lens)

# Find the maximum length of identifiers in the given text, excluding Python keywords
def extract_max_len_identifier(text):
    iden_lens = []
    identifiers = re.findall(r'[a-zA-Z_][a-zA-Z0-9_]*', text)
    for identifier in identifiers:
        if identifier not in keywords:
            iden_lens.append(len(identifier))
    if len(iden_lens) == 0:
        return 0
    return max(iden_lens)

# Extract unique lines of code from the given text
def extract_unique_lines_of_code(text):
    lines = text.split('\n')
    return set(lines)

# Calculate the Key Lines of Code (KLCID) metric for the given text
def klcid(text):
    unique_lines_code = extract_unique_lines_of_code(text)
    iden_count = []
    for line in unique_lines_code:
        iden_count.append(extract_identifier_count(line))
    unique_lines_code_with_iden = []
    for line in unique_lines_code:
        if extract_identifier_count(line) > 0:
            unique_lines_code_with_iden.append(line)
    if len(unique_lines_code_with_iden) == 0:
        return 0
    return sum(iden_count) / len(unique_lines_code_with_iden)

# Count the number of operands in the given string containing expressions and operators
def extract_operand_count(string):
    res = re.split(r'''[\-()\+\*\/\=\&\|\^\~\<\>\%]|
    \*\*|\/\/|
    \>\>|\<\<|
    \+\=|\-\=|\*\=|\/\=|\%\=|
    \/\/\=|\*\*\=
    \&\=|\|\=|\^\=|\>\>\=|\>\>\=|
    \=\=|\!\=|\>\=''', string)
    res = list(filter(None, res))
    return len(res)

# Count the number of unique operands in the given string containing expressions and operators
def extract_unique_operand_count(string):
    res = re.split(r'''[\-()\+\*\/\=\&\|\^\~\<\>\%]|
    \*\*|\/\/|
    \>\>|\<\<|
    \+\=|\-\=|\*\=|\/\=|\%\=|
    \/\/\=|\*\*\=
    \&\=|\|\=|\^\=|\>\>\=|\>\>\=|
    \=\=|\!\=|\>\=''', string)
    res = list(filter(None, res))
    return len(set(res))

# Count the number of arguments in Python function calls within the given string
def python_arguments(string):
    args = []
    args_regex = re.compile(
            r'''(
                [a-zA-Z_][a-zA-Z0-9_]*\((.*?)\)
            )''',
            flags=re.DOTALL | re.VERBOSE | re.MULTILINE
        )
    try:
        s = re.findall(args_regex, string)
        z = [i[0] for i in s]
        for i in z:
            args = args + re.search(r'\((.*?)\)',i).group(1).split(',')
        return len(args)
    except:
        return 0

# Count the number of loop statements (while and for) in the given text
def extract_loop_statements_count(text):
    keywords = ['while', 'for']
    count = 0
    for keywords in keywords:
        count += text.count(keywords)
    return count

# Count the number of if statements in the given text
def extract_if_statements_count(text):
    keywords = ['if']
    count = 0
    for keywords in keywords:
        count += text.count(keywords)
    return count

# Calculate the total number of loop and if statements in the given text
def statements_count(text):
    return extract_loop_statements_count(text) + extract_if_statements_count(text)

# Calculate the average nested depth of code blocks in the given text
def nested_depth(text):
    blocks = text.split('\n')
    depth = []
    for block in blocks:
        depth.append(block.count(' '))
    return sum(depth) / len(depth)

# Perform complexity analysis on the given source code using a custom ComplexityVisitor
def complexity_analysis2(source):
    source_new = source.replace('%matplotlib inline', '')
    try:
        v = ComplexityVisitor.from_code(source_new)
        return v.complexity
    except Exception as e:
        return 0
# Capture and extract import statements from the given source code
def capture_imports(source_code):
    import_regex = r"^\s*(?:from|import)\s+(\w+(?:\s*,\s*\w+)*)"
    # Find all import statements
    import_matches = re.findall(import_regex, source_code, re.MULTILINE)
    for i in import_matches:
        if ',' in i:
            new_i = i.replace(' ', '').split(',')
            import_matches.extend(new_i)
            import_matches.remove(i)
    return import_matches
Code_df['API'] = Code_df['source'].apply(lambda x: capture_imports(str(x))) 

# Extracting API values from 'Code_df', calculating their frequency, and computing an EAP (External API Popularity) score
l = Code_df['API'].values.tolist()
flat_list = [item for sublist in l for item in sublist]
eap_dict = dict(collections.Counter(flat_list))
eap_score_dict = dict(sorted(eap_dict.items(), key=lambda item: item[1], reverse=True))
max_freq = eap_dict['sklearn']
for k, v in eap_score_dict.items():
    eap_score_dict[k] = v / max_freq
def eap_score(ap_list):
    score = 0
    for i in ap_list:
        score += eap_score_dict.get(i, 0)
    return score
Code_df['EAP'] = Code_df['API'].progress_apply(lambda x: eap_score(set(x)))   

# Code Metrics
Code_df['LOC'] = Code_df['source'].apply(lambda x: x.count('\n')+1 if type(x)==str else 0)
Code_df['BLC'] = Code_df['LOC'].apply(lambda x:1 if x==0 else 0)
Code_df['UDF'] = Code_df['source'].apply(lambda x: sum([len(re.findall('^(?!#).*def ', y)) for y in x.split('\n')]) if type(x)==str else 0)
Code_df['I'] = Code_df['source'].apply(lambda x: x.count('import ') if type(x)==str else 0)
Code_df['EH'] = Code_df['source'].apply(lambda x: x.count('try:')  if type(x)==str else 0)
Code_df['ALLC'] = Code_df['source'].apply(lambda x: sts.mean([len(y) for y in x.split('\n')]) if type(x)==str else 0)
Code_df['NVD'] = Code_df['output_type'].progress_apply(lambda x: x.count('display_data') if type(x)==str else 0)
Code_df['NEC'] = Code_df['output_type'].progress_apply(lambda x: x.count('execute_result') if type(x)==str else 0)
Code_df['S'] = Code_df['source'].progress_apply(lambda x: statements_count(str(x)))
Code_df['P'] = Code_df['source'].progress_apply(lambda x: python_arguments(str(x)))
Code_df['KLCID'] = Code_df['source'].progress_apply(lambda x: klcid(str(x)))
Code_df['NBD'] = Code_df['source'].progress_apply(lambda x: nested_depth(str(x)))
Code_df['AOPERATOR'] = Code_df['source'].progress_apply(lambda x: extract_arithmatic_operator_count(str(x)))
Code_df['LOPERATOR'] = Code_df['source'].progress_apply(lambda x: extract_comparision_operator_count(str(x)))
Code_df['LOPRND'] = Code_df['source'].progress_apply(lambda x: boolean_logic_operator_count(str(x)))
Code_df['OPRND'] = Code_df['source'].progress_apply(lambda x: extract_operand_count(str(x)))
Code_df['UOPRND'] = Code_df['source'].progress_apply(lambda x: extract_unique_operand_count(str(x)))
Code_df['UOPRATOR'] = Code_df['source'].progress_apply(lambda x: extract_unique_operator_count(str(x)))
Code_df['ID'] = Code_df['source'].progress_apply(lambda x: extract_identifier_count(str(x)))
Code_df['ALID'] = Code_df['source'].progress_apply(lambda x: extract_avg_len_identifier(str(x)))
Code_df['MLID'] = Code_df['source'].progress_apply(lambda x: extract_max_len_identifier(str(x)))
Code_df ['CyC'] = Code_df['source'].progress_apply(lambda x: complexity_analysis2(str(x)))


############## COMMENT METRICS ##############

# Helper Methods for Comment Metrics

# Count the number of inline comments in the given string
def count_inline_comment(string: str) -> str:
    inline_regex = re.compile(
        r'''(
            (?<=\#).+ # comments like: # This is a comment
        )''',
        flags=re.VERBOSE
    )
    # return inline_regex.sub('', string)

    return len(re.findall(inline_regex, string))

# Extract and count the number of multi-line comments in the given string
def multi_line_comments(string: str) -> str:

    # Python comments
    multi_line_python_regex = re.compile(
        r'''(
            (?<=\n)\'{3}.*?\'{3}(?=\s*\n) |
            (?<=^)\'{3}.*?\'{3}(?=\s*\n) |
            (?<=\n)\'{3}.*?\'{3}(?=$) |
            (?<=^)\'{3}.*?\'{3}(?=$) |
            (?<=\n)\"{3}.*?\"{3}(?=\s*\n) |
            (?<=^)\"{3}.*?\"{3}(?=\s*\n) |
            (?<=\n)\"{3}.*?\"{3}(?=$) |
            (?<=^)\"{3}.*?\"{3}(?=$)
        )''',
        flags=re.DOTALL | re.VERBOSE | re.MULTILINE
    )
    python_multi_line_count = re.findall(multi_line_python_regex, string)


    return python_multi_line_count

# Count the total number of line comments (inline + multi-line) in the given string
def extract_line_comments(text):
    multi_lines_lines = []
    multi_lines = multi_line_comments(text)
    for line in multi_lines:
        multi_lines_lines += line.split('\n')
    return len(multi_lines_lines) + count_inline_comment(text) 

# Count the total number of words in line comments in the given string
def count_comment_word(text):
    comment_word_count = 0
    comments = re.findall(r'(?<=\#).+', text)
    for comment in comments:
        comment_word_count += len(comment.split())
    return comment_word_count

# Comment Metrics
Code_df['LOCom'] = Code_df['source'].progress_apply(lambda x: extract_line_comments(str(x)))
Code_df['CW'] = Code_df['source'].progress_apply(lambda x: count_comment_word(str(x)))

# Save code and comment cell metrics
print("###########SAVING CODE CELL METRICS...###########")
columns_to_drop = ['source', 'output_type', 'execution_count']
Code_df.drop(columns=columns_to_drop).to_csv(code_cell_metrics_dataframe, index=False)

# Free up memory
del Code_df


############## MARKDOWN METRICS ##############


# Count headers (H1, H2 , H3)
def header1_counter(text):
    count = 0
    if text[0:2] =='# ':
        count += 1
    for i in range(len(text)-2):
        if text[i:i+3] ==' # ' or text[i:i+3] == '\n# ':
            count +=1
    return count
def header2_counter(text):
    count = 0
    if text[0:3] =='## ':
        count += 1
    for i in range(len(text)-2):
        if text[i:i+4] ==' ## ' or text[i:i+4] == '\n## ':
            count +=1
    return count
def header3_counter(text):
    count = 0
    if text[0:4] =='### ':
        count += 1
    for i in range(len(text)-3):
        if text[i:i+5] ==' ### ' or text[i:i+5] == '\n### ':
            count +=1
    return count

# Count the number of words
def count_md_word(text):
    return len(text.split())

# Markdown Metrics
md_df['LMC'] = md_df['source'].apply(lambda x: len(re.findall('\n', x))+1 if type(x)==str else 0)
md_df['H1'] = md_df['source'].progress_apply(header1_counter)
md_df['H2'] = md_df['source'].progress_apply(header2_counter)
md_df['H3'] = md_df['source'].progress_apply(header3_counter)
md_df['MW'] = md_df['source'].progress_apply(lambda x: count_md_word(str(x)))

# Save markdown cell metrics
print("###########SAVING MARKDOWN CELL METRICS...###########")
columns_to_drop = ['source']
md_df.drop(columns=columns_to_drop).to_csv(markdown_cell_metrics_dataframe, index=False)