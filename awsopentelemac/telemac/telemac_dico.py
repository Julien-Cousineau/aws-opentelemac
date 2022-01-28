"""
Class for Telemac dictionary file manipulation
"""

from .tools import EMPTY_LINE, ENTRY_QUOTE, EXIT_SQUOTE, EXIT_DQUOTE,\
                            KEY_EQUALS, DICO_KEYS, EMPTY_LINE, VAL_EQUALS, \
                            convert_to_type
from .exceptions import TelemacException
import re

# Global variable containing parsed dictionaries
DICOS = {}

class TelemacDico(object):
    """
    Class to manipulation a Telemac-Mascaret dictionary
    """

    def __init__(self, file_name):
        """
        Init function

        @param file_name (string) Name of the dictionary
        """
        self.file_name = file_name
        self.data = {}
        self.fr2gb = {}
        self.gb2fr = {}
        self._scan_dico()

    def _scan_dico(self):
        """
        Scan the dictionnary and set default values
        """
        keylist = []
        with open(self.file_name, 'r', encoding="utf-8") as f:
            dico_lines = f.readlines()
        # Cleaning up dictonary
        # ~~ buddle continuations (long strings) and remove comments and empty
        # lines
        core = []
        i = -1
        while i < len(dico_lines) - 1:
            i = i + 1
            line = ''
            l = dico_lines[i].strip()
            # Empty line
            proc = re.match(EMPTY_LINE, l)
            if proc:
                continue
            # Comment line or special keyword
            if l[0] == '/' or l[0] == '&':
                continue
            # Beginning of a string if not 'after' will be empty
            proc = re.match(ENTRY_QUOTE, l)
            line = proc.group('before')
            l = proc.group('after').strip()
            # Merging the whole string into one line
            # TODO: Do not merge AIDE* keep the linebreaks
            while l != '':
                # Double quote string "....."
                if l[0:1] == '"':
                    proc = re.match(EXIT_DQUOTE, l+' ')
                    if proc:
                        # Replace single quote inside string by double one
                        line += "'" + proc.group('before').replace("'", '"') \
                                + "'"
                        # TODO: See if the lines below are useful
                        proc = re.match(ENTRY_QUOTE,
                                        proc.group('after').strip())
                        line += proc.group('before')
                        l = proc.group('after').strip()
                    else:
                        i = i + 1
                        l = l + ' ' + dico_lines[i].strip()
                # Single quote string ('.....')
                elif l[0:1] == "'":
                    proc = re.match(EXIT_SQUOTE, l+' ')
                    if proc:
                        # Replace single quote inside string by double one
                        line += "'" + proc.group('before').replace("'", '"') \
                                + "'"
                        proc = re.match(ENTRY_QUOTE,
                                        proc.group('after').strip())
                        line += proc.group('before')
                        l = proc.group('after').strip()
                    else:
                        i = i + 1
                        l = l + ' ' + dico_lines[i].strip()
            # Adding new merged line
            core.append(line)

        # Mergin all lines into a one line
        dico_stream = (' '.join(core)).replace('  ', ' ').replace('""', '"')
        # ~~ Identify key values for each keyword

        while dico_stream != '':
            # ~~ key
            proc = re.match(KEY_EQUALS, dico_stream)
            keyword = proc.group('key').strip()
            if keyword not in DICO_KEYS:
                raise TelemacException(\
                 'unknown key {} for {} '.format(keyword, proc.group('after')))

            dico_stream = proc.group('after')    # still hold the separator
            # ~~ val
            proc = re.match(VAL_EQUALS, dico_stream)
            if not proc:
                raise TelemacException('no value to keyword '+keyword)
            val = []
            # Finding values (loop for multiple values ; separated)
            while proc:
                # If string removing single quote beofre and after value
                if proc.group('val')[0] == "'":
                    val.append(proc.group('val')[1:-1])
                else:
                    val.append(proc.group('val'))
                dico_stream = proc.group('after')    # still hold the separator
                proc = re.match(VAL_EQUALS, dico_stream)
            keylist.append([keyword, val])


        # ~~ Group pairs of keyword/val by each occurence of NOM
        while keylist != []:
            if keylist[0][0] != 'NOM' and keylist[1][0] != 'NOM1':
                raise TelemacException('could not read NOM or NOM1 '
                                'from {}'.format(keylist[0][1]))

            fr_name = keylist[0][1][0].replace('"', "'")
            gb_name = keylist[1][1][0].replace('"', "'")
            self.fr2gb[fr_name] = gb_name
            self.gb2fr[gb_name] = fr_name
            self.data[gb_name] = {}
            keylist.pop(0)
            keylist.pop(0)
            while keylist != []:
                if keylist[0][0] == 'NOM':
                    break
                val = keylist[0][1]
                self.data[gb_name][keylist[0][0]] = val
                keylist.pop(0)

        # converting each key value to its proper format
        for keyword, key_info in self.data.items():
            key_info['TYPE'] = key_info['TYPE'][0]
            for key in key_info:
                if key == 'TYPE':
                    continue

                if key in ['DEFAUT', 'DEFAUT1']:
                    key_info[key] = convert_to_type(key_info['TYPE'],
                                                    key_info[key])
                elif key in ['INDEX', 'NIVEAU', 'TAILLE']:
                    key_info[key] = convert_to_type('INTEGER',
                                                    key_info[key])
                elif key in ['CHOIX', 'CHOIX1']:
                    # We have two type of choix integer or string
                    # The integer ones follow this syntax for each value:
                    # index="comment"
                    # Integer type
                    if '=' in key_info[key][0]:
                        values = key_info[key]
                        key_info[key] = {}
                        # Filling a dictionary with index:comment as
                        # index/values
                        for val in values:
                            str_index, comment = val.split('=', maxsplit=1)
                            key_info[key][str_index.strip(' ')] = \
                                     comment.strip('"')
                    else:
                        # List of strings just removing quotes for each value
                        key_info[key] = \
                             [val.strip("'\"") for val in key_info[key]]
                # AIDE*, APPARENCE, CHOIX*, COMPORT
                # COMPOSE, CONTROLE, RUBRIQUE*, TYPE, SUBMIT
                elif len(key_info[key]) == 1:
                    key_info[key] = key_info[key][0]

    def __str__(self):
        """
        Ascii representation of dicotionnary
        """
        #TODO: Make a fancy following section order
        string = "Printing: " + self.file_name + "\n\n"
        for key in self.data:
            string += "~> Key: {}\n   fr {}\n".format(key, self.gb2fr[key])
            for keyword in DICO_KEYS:
                if keyword in self.data[key]:
                    # Specific display for CHOIX and CHOIX1
                    if keyword in ['CHOIX', 'CHOIX1']:
                        string += '   {} = \n'.format(keyword)
                        # Integer choix
                        if isinstance(self.data[key][keyword], dict):
                            for idx, comment in self.data[key][keyword].items():
                                string += '   -  {} : {}\n'.format(idx, comment)
                        # String choix
                        else:
                            for val in self.data[key][keyword]:
                                string += '   -  {}\n'.format(val)


                    else:
                        string += "   {} = {}\n"\
                                  .format(keyword, self.data[key][keyword])

        return string
