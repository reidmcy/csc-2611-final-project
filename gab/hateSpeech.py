import hatesonar

import re
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module='sklearn')

sonar = hatesonar.Sonar()

def isHate(text, get_probs = False):
    ret = sonar.ping(text = text)
    if get_probs:
        return ret['top_class'], {v['class_name'] : v['confidence'] for v in ret['classes']}
    else:
        return ret['top_class']

#From Hatebase, hatebase.org
antiSemticTerms = [
    "Christ killer",
    "Christ killers",
    "four by two",
    "four by twos",
    "gew",
    "gews",
    "globalist",
    "globalists",
    "hebe",
    "hebes",
    "hebro",
    "hebros",
    "heeb",
    "heebs",
    "holohoax",
    "hymie",
    "hymies",
    "jew-fucked",
    "jew-fucker",
    "jewbag",
    "jewbagg",
    "jewtard",
    "jewtarded",
    "khazar",
    "khazars",
    "kike",
    "kikesberg",
    "oven dodger",
    "oven dodgers",
    "shekelnose",
    "Zio",
    "ziojew",
    "ziojews",
    "Zionazi",
    "Zionazis",
]

antiSemticTerms = [t.lower() for t in antiSemticTerms]
antiSemticTerms += [t +'s' for t in antiSemticTerms if not t.endswith('s')]
antiSemticTerms = set(antiSemticTerms)

antiSemticRegex = re.compile("|".join(antiSemticTerms))

antiMuslimTerms = [
    "African catfish",
    "African catfishes",
    "Bengali",
    "Bengalis",
    "camel fucker",
    "camel fuckers",
    "derka derka",
    "derka derkas",
    "durka durka",
    "durka durkas",
    "Gerudo",
    "Gerudos",
    "Jihadi",
    "Jihadis",
    "musla",
    "muslamic",
    "muslimal",
    "muslimic",
    "mussie",
    "mussies",
    "muzzie",
    "muzzies",
    "muzzpig",
    "muzzpigs",
    "muzzrat",
    "muzzrats",
    "muzzy",
    "pisslam",
    "sand monkey",
    "sand monkeys",
]

antiMuslimTerms = [t.lower() for t in antiMuslimTerms]
antiMuslimTerms += [t +'s' for t in antiMuslimTerms if not t.endswith('s')]
antiMuslimTerms = set(antiMuslimTerms)

antiMuslimRegex = re.compile("|".join(antiMuslimTerms))
