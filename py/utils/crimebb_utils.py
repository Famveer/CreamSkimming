import re

def filter_attachment(text, pattern, replacement_string=" FAMVEER "):
    # Define the regular expression pattern to match text between ***LINK*** and ***LINK***
    pattern_ = pattern.replace("*", "\*")
    #pattern = r'\*\*\*LINK\*\*\*(.*?)\*\*\*LINK\*\*\*'
        # Use re.findall() to extract the text between ***LINK*** and ***LINK***
    new_pattern = f'{pattern_}(.*?){pattern_}'
    links = re.findall(f'{new_pattern}', text, flags=re.DOTALL)
    
    return links

def replace_attachment(text, pattern_list, replacement_string=" FAMVEER "):
    for pattern in pattern_list:
        pattern_ = pattern.replace("*", "\*")
        new_pattern = f'{pattern_}(.*?){pattern_}'
        text = re.sub(f'{new_pattern}', replacement_string, text, flags=re.DOTALL)
        
    return text
