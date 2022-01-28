"""
Class for Telemac steering file manipulation
"""

from .tools import KEY_COMMENT, EMPTY_LINE, KEY_EQUALS, VAL_EQUALS, \
                            KEY_NONE, convert_to_type, check_type, format72
from .telemac_dico import TelemacDico
from .exceptions import TelemacException
import re
from os import path,walk
import shutil
import itertools
flatten = itertools.chain.from_iterable


SPECIAL = ['VARIABLES FOR GRAPHIC PRINTOUTS',
           'VARIABLES FOR 3D GRAPHIC PRINTOUTS',
           'VARIABLES FOR 2D GRAPHIC PRINTOUTS',
           'VARIABLES TO BE PRINTED',
           'COUPLING WITH',
          ]

def get_dico(module):
    """
    Returns path of the dictionary for a given module

    @param module (str) name of a telemac-mascaret module

    @returns the path
    """
    Filename="dico/{}.dico".format(module)
    Filename=path.join(path.dirname(path.realpath(__file__)),Filename)
    return Filename

class TelemacCas(object):
    """
    Class to hanlde a Telemac-mascaret steering file
    """

    def __init__(self, module,file_name=None):
        """
        Init of the class

        @param file_name (string) Name of the steering file
        @param dico_file (string) Name of the dictionary to use
        """
        
        #TODO: Add identification of the module
        self.values = {}
        self.lang = 'en'
        self.in_files = {}
        self.out_files = {}
        self.dico = TelemacDico(get_dico(module))
        if file_name is not None:
            self.file_name = file_name
            self.file_dir  = path.dirname(self.file_name)
            # Reading data from file
            self._parse_cas()
            self._check()
            
        else:self.file_name = ""
        
    def setValues(self,values):
        # for v in values:
          # values[v]=[values[v]]
        self.values=values
        self._check_choix()
        self._set_io_files()
    
    def _check(self):
        # Getting language info
        # self._identify_lang()
        # Checking that all the keyword are from the dictionary
        self._check_content()
        # Convert content according to its type
        self._convert_values()
        # Checking that values are in CHOIX/CHOIX1
        self._check_choix()
        # Identify input and ouput files
        self._set_io_files()
        

    def _parse_cas(self):
        """
        Parse the steering file and identify (key, value)
        And updates self.values according to the pairs identified
        """
        lines = []
        # ~~ clean ending empty lines
        with open(self.file_name, 'r') as f:
            for line in f.readlines():
                # Remove trailing spaces (left and right) and \n
                line = line.strip(' ').rstrip('\n')
                # not adding empty lines
                if line != '':
                    # Skipping &key (&ETA, &FIN...)
                    if line[0] == '&':
                        continue
                    else:
                        lines.append(line)

        # ~~ clean comments
        core = []
        for line in lines:
            line = line.replace('"""', "'''")\
                       .replace('"', "'")\
                       .replace("''", '"')
            proc = re.match(KEY_COMMENT, line+'/')
            line = proc.group('before').strip() + ' '
            proc = re.match(EMPTY_LINE, line)
            if not proc:
                core.append(line)

        # Creates a one line of the cleaned up steering
        cas_stream = (' '.join(core))
        # ~~ Matching keword -> values
        while cas_stream != '':
            # ~~ Matching keyword
            proc = re.match(KEY_EQUALS, cas_stream)
            if not proc:
                raise TelemacException(\
                   ' Error while parsing steering file {} '
                   'incorrect line:\n{}'\
                   .format(self.file_name, cas_stream[:100]))
            keyword = proc.group('key').strip()
            cas_stream = proc.group('after')    # still hold the separator
            # ~~ Matching value
            proc = re.match(VAL_EQUALS, cas_stream)
            if not proc:
                raise TelemacException('No value to keyword '+keyword)
            val = []
            # The value can be on multiple lines
            while proc:
                if proc.group('val') == '"':
                    val.append('')
                else:
                    val.append(proc.group('val').replace("'", ''))
                cas_stream = proc.group('after')    # still hold the separator
                proc = re.match(VAL_EQUALS, cas_stream)
            # Updating the value with the last one read
            self.values[keyword] = val

    def _identify_lang(self):
        """
        Identifying the language of the steering file
        """
        # Identifying language
        # Looping on all the keywords of the steering file
        # And looking for the first keyword that is in only french/english
        for key in self.values:
            # Skipping keyword that are the same in french and english
            if key in self.dico.fr2gb and key in self.dico.gb2fr:
                continue
            if key in self.dico.fr2gb:
                self.lang = 'fr'
            else:
                self.lang = 'en'
            break

    def _check_content(self):
        """
        Checks that all the keywords are from the dictionary
        """
        if self.lang == 'en':
            key_list = self.dico.gb2fr
        else:
            key_list = self.dico.fr2gb

        for key in self.values:
            if key not in key_list:
                raise TelemacException(\
                    "Unknown keyword {} in steering file {}"\
                    .format(key, self.file_name))

    def _check_choix(self):
        """
        Check if the keyword value is in the list of choix
        """
        for key, value in self.values.items():
            # If empty value doing nothing
            if value == '' or value == []:
                continue
            # Check if we have a keyword with choices
            choix = 'CHOIX1' if self.lang == 'en' else 'CHOIX'
            if choix in self.dico.data[key]:
                list_choix = self.dico.data[key][choix]
                # Special treatment for grapchi outputs like keywords
                if key in SPECIAL:
                    if key == 'COUPLING WITH':
                        list_val = value.split(";")
                    else:
                        list_val = value.split(",")
                    for val in list_val:
                        tmp_val = str(val.strip(' 0123456789*'))
                        new_val = 'k' + tmp_val + 'i'
                        new_val2 = 'k' + tmp_val
                        new_val3 = tmp_val + 'i'
                        # Handling case of Tracer lists such as T1, T* ...
                        # Special case for gaia where you have stuff like kSi
                        # and kES where k and i are number or *
                        if not(str(val).strip(' ') in list_choix or \
                           str(val).rstrip('123456789*') in list_choix or \
                           new_val in list_choix or \
                           new_val2 in list_choix or \
                           new_val3 in list_choix):
                            raise TelemacException(\
                       "In {}: \n".format(self.file_name)+
                       "The value for {} ({})is not among the choices: \n{}"\
                       .format(key, val, list_choix))
                elif isinstance(value, list):
                    for val in value:
                        if str(val).strip(' ') not in list_choix:
                            raise TelemacException(\
                         "In {}: \n".format(self.file_name)+
                         "The value for {} ({})is not among the choices: \n{}"\
                         .format(key, val, list_choix))
                else:
                    if str(value).strip(' ') not in list_choix:
                        raise TelemacException(\
                         "In {}: \n".format(self.file_name)+
                         "The value for {} ({})is not among the choices: \n{}"\
                         .format(key, value, list_choix))

    def _convert_values(self):
        """
        Convert string value to its Python type and replace key by english key

        @param keyword (string) Name of the dico keyword
        @param value (string) Value given in the case file

        @return Value in its proper type (int/float/boolean/string)
        """
        

        # Updating values dict
        # Converting value and translating key if necessary
        # Using a copy of keys as the loop removes some elements
        list_keys = list(self.values.keys())
        for keyword in list_keys:
            value = self.values[keyword]
            if self.lang == 'en':
                gb_keyword = keyword
            else:
                gb_keyword = self.dico.fr2gb[keyword]

            key_info = self.dico.data[gb_keyword]

            # Convert according to type of the keyword
            if self.lang == 'fr':
                # All keys in values are to be in english
                del self.values[keyword]
            
            if keyword!="FORTRAN FILE":
              self.values[gb_keyword] = convert_to_type(key_info['TYPE'], value)  
            else:
              if self.file_name!="":
                self.values[gb_keyword]=self.getFilePath(value[0])
              else:
                self.values[gb_keyword] = value
    
    def _getFilePath(self,value):
      value = value.strip("'")
      if not path.exists(value):
        value = path.join(path.dirname(self.file_name),value)
        if not path.exists(value):raise TelemacException("File does not exist: {}".format(value))  
      if path.isdir(value):
        for root, dirs, files in walk(value):
          value=[path.join(root, name) for name in files]
      return value              
    
    def getFilePath(self,value):
      if isinstance(value,list):
        return [self._getFilePath(l) for l in value]
      return self._getFilePath(value)
      
    def _set_io_files(self):
        """
        Detect input and ouput files from the steergin file data
        Checks that input files exists as well
        """
        for key in self.values:
          # Identify a file keyword (they have a SUBMIT key)
          key_data = self.dico.data[key]
          if 'SUBMIT' in key_data:
              # input file
              if 'LIT' in key_data['SUBMIT']:
                self.in_files[key] = key_data['SUBMIT']
                if self.file_name:
                  self.values[key]=self.getFilePath(self.values[key])
                  
              # output file
              if 'ECR' in key_data['SUBMIT']:
                  self.out_files[key] = key_data['SUBMIT']

    def write(self, cas_file):
        """
        Write content of class in ascii for into a file
        """
        # TODO: fancier write using rubrique
        with open(cas_file, 'w') as f:
            for key in sorted(self.values.keys()):
                val = self.values[key]
                if self.lang == 'fr':
                    real_key = self.dico.gb2fr[key]
                else:
                    real_key = key
                
                if key=="FORTRAN FILE":
                  f.write("FORTRAN FILE = 'user_fortran'\n")
                elif isinstance(val, list):
                    s_val = [repr(item) for item in val]
                    string = "{} = {}\n".format(real_key, ";".join(s_val))
                    if len(string) < 73:
                        f.write(string)
                    else:
                        string = "{} = \n{}\n".format(real_key,
                                                      ";\n".join(s_val))
                        f.write(string)
                else:
                    string = "{} = {}\n".format(real_key, repr(val))
                    if len(string) < 73:
                        f.write(string)
                    else:
                        string = "{} =\n{}\n".format(real_key,
                                                     format72(repr(val)))
                        f.write(string)

    
    
    def __str__(self):
        """ str function """
        string = "~~ " + self.file_name + "\n"
        string = "   Language: " + self.lang + "\n"
        for key in sorted(self.values.keys()):
            string += "{} = {}\n".format(key, self.values[key])
        string += "input files:\n"
        for key in self.in_files:
          value=self.values[key]
          if isinstance(value,list):
            string += "  " + ",".join([l.strip("'") for l in value]) + "\n"
          elif isinstance(value,str):
            string += "  " + self.values[key] + "\n"
          else: string += "  NONE\n"
        string += "output files:\n"
        for key in self.out_files:
            string += "  " + self.values[key].strip("'") + "\n"

        return string
