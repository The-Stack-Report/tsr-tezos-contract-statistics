from constants.dirs import cache_dir
import os
import pandas as pd
xtz_attributions_cache_file_path = cache_dir / "join_tables" / "xtz_attributions.parquet"

def run(params={}):
    attribution_batches_dir = cache_dir / "xtz_attribution_batches"

    xtz_attributions_cache_df = False
    if os.path.exists(xtz_attributions_cache_file_path):
        xtz_attributions_cache_df = pd.read_parquet(xtz_attributions_cache_file_path)
    else:
        print("xtz attributions cache not initialized yet, initializing.")
        xtz_attributions_cache_df = pd.DataFrame([], columns=["sender_op_hash_counter_nonce"])

    cached_xtz_attribution_batch_files = os.listdir(attribution_batches_dir)
    cached_xtz_attribution_batch_files = [f for f in cached_xtz_attribution_batch_files if f.endswith(".parquet")]
    
    if len(cached_xtz_attribution_batch_files) == 0:
        # No cached attribution files, so returning.
        return True
    
    # Load batch attributions file and merge into 1 df.
    batched_attributions = []
    for f in cached_xtz_attribution_batch_files:
        f_path = cache_dir / "xtz_attribution_batches" / f
        print(f_path)
        batch_df = pd.read_parquet(f_path)
        batched_attributions.append(batch_df)


    # Filter out already found attributions based on existing xtz_attributions_cache_df index.
    
    new_attributions_df = pd.concat(batched_attributions)

    new_attributions_df = new_attributions_df[~new_attributions_df.index.isin(xtz_attributions_cache_df.index)]

    print(f"New attributions found: {len(new_attributions_df)}")

    # Concat newly found attributions to xtz_attributions_cache_df

    xtz_attributions_cache_df = pd.concat([
        xtz_attributions_cache_df,
        new_attributions_df
    ])

    xtz_attributions_cache_df.to_parquet(xtz_attributions_cache_file_path)

    # Delete previous batches
    for filename in os.listdir(attribution_batches_dir):
        file_path = os.path.join(attribution_batches_dir, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))