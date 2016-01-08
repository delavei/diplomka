# Makefile na preklad Texovych zdrojovych kodov
#
# Copyright (c) 2013, Juraj Lezdik
#

THESIS=dip-lezdik

all: pdf

clean:
	-rm *.*~ *.toc *.lof *.aux  *.log *.pdf *.blg *.bbl *.idx *.ilg *.ind *.out

pdf: $(THESIS).pdf

$(THESIS).pdf: $(THESIS).tex
	pdflatex -output-format=pdf $(THESIS)
	pdflatex -output-format=pdf $(THESIS)
	bibtex $(THESIS)
	pdflatex -output-format=pdf $(THESIS)
	@while grep -qs 'LaTeX Warning:.*references' $(THESIS).log; do echo; pdflatex -output-format=pdf $(THESIS); done
	mv $(THESIS).pdf $(THESIS)-in.pdf
	pdfopt $(THESIS)-in.pdf $(THESIS).pdf
	rm $(THESIS)-in.pdf
	mv $(THESIS).pdf ./output/$(THESIS).pdf
	
 
