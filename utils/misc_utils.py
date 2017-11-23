'''
Created on 22 Nov 2017

@author: martinr
'''

import re

def getInputData(path):
    '''Returns file pointed to by path as 
       a list of lines.
    '''
    with open(path,'r') as content_file:
        content = list(content_file.read().splitlines())
    return content

def stripMargin(text):
    '''Strip the left margin from lines (text)
    
        Works the same way a stripMargin in Scala
    '''
    return re.sub('\n[ \t]*\|', '\n', text)


#TODO: Add unitest
if __name__ == '__main__':
    pass