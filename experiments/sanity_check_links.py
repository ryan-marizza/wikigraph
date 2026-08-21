from wikigraph.links import extract_links

wt = """{{Infobox animal|genus=[[Canis]]}}
The '''dog''' (from [[Latin]] ''canis'') is a [[domestication|domesticated]] [[gray wolf]].<ref>[[Cited]]</ref>"""

FLAGS = ('in_parens', 'in_italics', 'in_template', 'in_table',
         'in_ref', 'in_infobox', 'in_file_caption')

for link in extract_links(wt):
    flags = ','.join(k[3:] for k in FLAGS if link[k])
    print(f"{link['ordinal']:>2}  {link['target_raw']:<24} {flags}")
