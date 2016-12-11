# Makefile na preklad Texovych zdrojovych kodov
#
# Copyright (c) 2013, Juraj Lezdik
#

THESIS=dip-lezdik

all: pdf

clean:
	-rm *.toc *.lof *.aux  *.log *.pdf *.blg *.bbl *.out *.bcf *.lot *.xml

pdf: $(THESIS).pdf

$(THESIS).pdf: $(THESIS).tex
	pdflatex -output-format=pdf $(THESIS).tex
	biber $(THESIS)
	pdflatex -output-format=pdf $(THESIS).tex
	pdflatex -output-format=pdf $(THESIS).tex	


	
 
