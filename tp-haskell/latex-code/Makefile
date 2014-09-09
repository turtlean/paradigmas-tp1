
.PHONY: all dot clean

PDF=Informe.TP2.AyEDIII.2014.C1.pdf
TEXFILES=

# IMAGES=$(patsubst  %, image/%, ${IMAGE})

all: $(PDF)

$(PDF): ej1.tex ej2.tex ej3.tex introduccion.tex
	texi2pdf informe.tex -o $(PDF)

clean:
	rm -f $(PDF) *.aux *.log *.toc informe.out informe.blg informe.bbl
