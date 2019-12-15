jupyter-nbconvert Slides.ipynb --to=slides --reveal-prefix=ext/reveal.js --output=index
cat index.slides.html | head -n -4 > index.html
cat footer >> index.html

rm index.slides.html

sed -i -ne '/<body>/ {p; r background_image' -e ':a; n; /<h1/ {p; b}; ba}; p' index.html

git add index.html
git commit -m "$1"
git push origin gh-pages 

