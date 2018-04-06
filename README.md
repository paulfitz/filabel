Package up an ad-hoc list of files and their labels for use
in training a classifier.

From command line:

```
filabel labels dandelion geranium rose
filabel add dandelion images/d/*.jpg PICS/*.jpeg
filabel add geranium images/g/*.jpg extra/geranium*.png
filabel add rose images/r/*.jpg garden/rose/*
filabel splits train validation test
filabel add train images/*/*.jpg
filabel move train validation 25
filabel add test PICS/*.jpeg extra/geranium*.png garden/rose/*
filabel list
filabel list --json
```

Then from code:

```py
from filabel import FileLabel

fl = FileLabel()
split = fl.get_split('validation')
print(split.labels)
print(split.samplesForLabel('dandelion'))
print(split.samplesForLabel('geranium'))
```
