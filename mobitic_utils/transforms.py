"""
Request data from s3 application
"""
import pandas as pd


def calculate_average_weeks(population: pd.DataFrame) -> pd.DataFrame:
    """Calculate average estimated population per day of week and hour

    Args:
        population (pd.DataFrame): The clean population table

    Returns:
        pd.DataFrame: Averaged per day of week and hour estimated population
    """

    dates = population.columns
    average_week = population.T.groupby([dates.dayofweek, dates.hour]).agg("mean").T
    average_week.columns = [
        str(dayofweek) + "-" + str(hour) for dayofweek, hour in average_week.columns
    ]
    return average_week.reset_index()


def format_total_population(population: pd.DataFrame) -> pd.DataFrame:
    """Format index to insert into

    Args:
        population (pd.DataFrame): The clean population table

    Returns:
        pd.DataFrame: Averaged per day of week and hour estimated population
    """

    dates = population.columns
    population = population.T
    population = population.set_index(
        [dates.isocalendar().week, dates.dayofweek, dates.hour]
    )
    population = population.unstack(0).swaplevel(0, 1, 1).sort_index(1).T
    population.columns = [
        str(dayofweek) + "-" + str(hour) for dayofweek, hour in population.columns
    ]
    return population.reset_index()
