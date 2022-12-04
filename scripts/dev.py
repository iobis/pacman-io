from pacmanio.archive import Archive

archive = Archive("~/Desktop/temp/miri/site_1")

#print(archive.template.metadata.df)
#print(archive.template.samples.df)
#print(archive.template.vouchers.df)

archive.generate_dwca()
