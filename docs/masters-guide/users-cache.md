# User's Cache

Use `kts.save(obj/df, 'name')` to save DataFrames or objects.

```python
train = pd.read_csv('../input/train.csv').set_index('ID')
test = pd.read_csv('../input/test.csv').set_index('ID')

kts.save(train, 'train')
kts.save(test, 'test')
```

After you do so, you can leave only the following lines in the beginning of the notebook which will run much faster:

```python
train = kts.load('train')
test = kts.load('test')
```



