import xmltodict


def parse_into_dict(xmlfile):
    with open(xmlfile) as ifp:
        doc = xmltodict.parse(ifp.read())
        return doc
