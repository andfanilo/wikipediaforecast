# -*- coding: utf-8 -*-
import os
import click
import logging
from path import Path
import pandas as pd


@click.command()
@click.argument('input_dir', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(exists=True))
def main(input_dir, output_dir):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)

    raw_path = Path(input_dir) / 'train_1.csv.zip'

    if not raw_path.isfile():
        raise FileNotFoundError("train_1.csv.zip doesn't exist in {}".format(input_dir))

    logger.info("Read raw file {}".format(raw_path))
    raw = pd.read_csv(raw_path, compression='zip', encoding='iso-8859-1')

    logger.info('Convert views to integers')
    for col in raw.columns[1:]:
        raw[col] = pd.to_numeric(raw[col], downcast='integer')

    logger.info('Parsing page names in raw file')
    page_details = pd.DataFrame(raw['Page'].apply(parsePage))
    page_details.columns = ["agent", "access", "project", "pagename"]

    logger.info('Create final dataset')
    df = pd.concat([raw, page_details], axis=1)

    output_path = Path(output_dir) / 'df.csv'
    logger.info('Writing to {}'.format(output_path))
    df.to_csv(output_path, encoding='utf-8', index=False)


def parsePage(page):
    input = str(page).split('_')
    output = []
    output.append(input.pop())
    output.append(input.pop())
    output.append(input.pop())
    output.append('_'.join(input))
    return pd.Series(output)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    main()
