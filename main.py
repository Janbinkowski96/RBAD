import click
from tqdm import tqdm

from utils.cli import clear_
from utils.cli import print_
from utils.cli import print_help
from utils.cli import check_flags

from source.data_processor import DataProcessor


@click.command()
@click.option('--help', '-h',
              is_flag=True,
              expose_value=False,
              is_eager=False,
              callback=print_help,
              help="Print help message")
@click.option('--my-norm', '-m', type=click.Path(exists=True), help='Path to myNorm file in "csv" format.')
@click.option('--sample-sheet', '-s', type=click.Path(exists=True), help='Path to sample sheet file in "csv" format.')
@click.option('--output-file', '-o', type=click.Path(), help='Path to file directory.')
@click.pass_context
def cli(ctx, my_norm: str, sample_sheet: str, output_file: str) -> None:
    check_flags(ctx, my_norm, sample_sheet, output_file)

    clear_()
    processor = DataProcessor()
    processor.load_data(my_norm_path=my_norm, sample_sheet_path=sample_sheet)
    processor.check_data_view()
    clear_()
    column_to_split = processor.get_column_to_split()
    processor.merge(column_to_split=column_to_split)
    print_("Splitting data into groups")
    frames_of_data = processor.split_data(column_to_split=column_to_split)
    clear_()

    print_("Filtering 1.")
    reduced_frames = []
    for unique_class, mynorm in tqdm(frames_of_data.items()):
        threshold = processor.set_var_threshold(my_norm=mynorm)
        selected_my_norm = processor.reduce(my_norm=mynorm, var_threshold=threshold)
        reduced_frames.append(selected_my_norm)

    clear_()
    print_("Filtering 2.")
    processor.universal_merge(list_of_frames=reduced_frames)
    threshold = processor.set_var_threshold(my_norm=processor.my_norm)
    processor.my_norm = processor.reduce(my_norm=processor.my_norm, var_threshold=threshold, selection_type=1)
    processor.merge(column_to_split=column_to_split)
    processor.save_data(output_file_path=output_file)
    clear_()
    print_(processor.stats)


if __name__ == '__main__':
    cli()
