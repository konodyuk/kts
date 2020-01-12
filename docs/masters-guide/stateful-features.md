# Stateful Features

Some features need their internal state to be stored between train and inference stages. Inside a function which is wrapped with `@feature` you can use `df.train` and `df.state` as attributes of a dataframe passed as an argument:

```python
@feature
def stateful_feature(df):
    col_to_encode = 'text'
    from sklearn... import TfIdfVectorizer
    if df.train:
        # create a new encoder and fit it
        # then save in df.state
        enc = TfIdfVectorizer()
        tmp = enc.fit_transform(df[col_to_encode])
        df.state = enc
    else:
        # load existing encoder and use it
        enc = df.state
        tmp = enc.transform(df[col_to_encode])
    res = pd.DataFrame(data=tmp, 
                       index=df.index, 
                       cols=[f'tfidf_{col_to_encode}_{i}' for i in range(tmp.shape[1])]
    return res
```

You can also use an external dataframe inside a feature constructor. To do that, just pass a name of such a dataframe in cache:

```python
clients = pd.read_csv('../input/clients.csv')
kts.save(clients, 'clients')

@feature
def feature_using_external(df, clients='clients'):
    """Client's age for each transaction."""
    res = stl.empty_like(df)
    res['client_id'] = df['client_id']
    res = pd.merge(res, clients[['client_id', 'age']])
    return stl.drop(['client_id'])(res)
```

Use `kts.pbar` to track progress during computing:

```python
@feature
def feature_with_progressbar(df):
    """Create column interactions."""
    cols = ['a', 'b', 'c', 'd', 'e', 'f']
    res = stl.empty_like(df)
    for i in pbar(range(len(cols))):
        for j in range(i + 1, len(cols)):
            first_col = cols[i]
            second_col = cols[j]
            res[f'{first_col}_div_{second_col}'] = df[first_col] / df[second_col]
    return res
```

{% hint style="warning" %}
As your features are computed in separate processes, only `tqdm` or similar tools won't work. So use `kts.pbar`.
{% endhint %}

