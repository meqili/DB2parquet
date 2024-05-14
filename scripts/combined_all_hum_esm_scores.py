import pandas as pd
import os

n = 1
esm_variants = []

directory = "ALL_hum_isoforms_ESM1b_LLR_transpose"
for filename in os.listdir(directory):
    if n % 10000 == 0:
        esm_variants_df = pd.concat(esm_variants, ignore_index=True)
        esm_variants_df.to_csv('ALL_hum_isoforms_ESM1b_LLR_transpose_' + 'chunk_' + str(n) + '.csv', index=False)
        esm_variants = []
    df = pd.read_csv(os.path.join(directory, filename))
    df['uniprot_id'] = filename.replace('_LLR.csv', '')
    esm_variants.append(df)
    n = n + 1
esm_variants_df = pd.concat(esm_variants, ignore_index=True)
esm_variants_df.to_csv('ALL_hum_isoforms_ESM1b_LLR_transpose_' + 'chunk_' + str(n) + '.csv', index=False)