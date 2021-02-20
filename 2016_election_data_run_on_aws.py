def election_campaign_data(election_data_file, election_data_header_file, output):

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as sum, col as col, when as when

    spark = SparkSession.builder \
        .appName("Write") \
        .getOrCreate()

    donation_data_header_df = spark.read.option("header", "true") \
        .csv(election_data_header_file)

    donation_data_file_df = spark.read \
        .option("header", "false") \
        .option("sep", "|") \
        .csv(election_data_file)

    donation_data_merged_df = donation_data_header_df.union(donation_data_file_df)

    # Filter Data -  we are interested in these three campaigns.
    def filter_data_on_campaign_id(donation_data_df):
        # C00575795 - HILLARY FOR AMERICA
        # C00577130 - Bernie Sanders committee (BERNIE)
        # C00580100 - DONALD J. TRUMP FOR PRESIDENT, INC
        campaign_commite_id_list = ['C00575795', 'C00577130', 'C00580100']
        return donation_data_df[donation_data_df['CMTE_ID'].isin(campaign_commite_id_list)]

    donation_data_df = filter_data_on_campaign_id(donation_data_merged_df)

    # 1. How many donations did each campaign have?
    def number_of_donations_per_campaign(donation_data_df):
        # Filter negative amounts. Assuming this as return of donations.
        donation_data_df = donation_data_df.filter(donation_data_df['TRANSACTION_AMT'] > 0)
        number_of_donations_grouped = donation_data_df.groupBy('CMTE_ID').count()
        number_of_donations_grouped.coalesce(1).write.mode('overwrite').format('csv') \
            .option('header', 'true') \
            .save(output+'/'+'number_of_donations_per_campaign')
        return number_of_donations_grouped

    number_of_donations_data = number_of_donations_per_campaign(donation_data_df)
    number_of_donations_data.show()

    # 2. What was the total amount donated to each campaign?
    def donation_amount_per_campaign(donation_data_df):
        # Considering the negative numbers as well. This is helpfull in calculating final donation amount.
        donation_amount = donation_data_df.groupBy('CMTE_ID').agg(sum('TRANSACTION_AMT').alias("TOTAL_DONATION"))
        donation_amount.coalesce(1).write.mode('overwrite').format('csv') \
            .option('header', 'true') \
            .save(output+'/'+'donation_amount_per_campaign')
        return donation_amount

    donation_amount_per_campaign_data = donation_amount_per_campaign(donation_data_df)
    donation_amount_per_campaign_data.show()

    # 3. What percentage of the each campaigns donations was done by small contributors?
    def small_donation_percentage(donation_data_df):
        # Filter negative donations
        positive_donations_df = donation_data_df.filter(donation_data_df['TRANSACTION_AMT'] > 0)
        # Filter small donations and find the percentage
        small_donations_percentage_df = positive_donations_df.groupBy('CMTE_ID') \
            .agg(((sum(when(col('TRANSACTION_AMT') < 200, \
                            col('TRANSACTION_AMT'))) / sum(col('TRANSACTION_AMT'))) * 100).alias("SMALL_DONATIONS_PERCENTAGE"))
        small_donations_percentage_df.coalesce(1).write.mode('overwrite').format('csv')\
            .option('header', 'true') \
            .save(output+'/'+'small_donations_percentage')
        return small_donations_percentage_df

    small_donations_detail = small_donation_percentage(donation_data_df)
    small_donations_detail.show()

    # 4. Produce a histogram of the donations for each campaign?
    # For this we will be downloading the data for three campaigns and plotting locally.
    donation_data_df = donation_data_df.select('CMTE_ID', 'TRANSACTION_AMT', 'NAME')
    donation_data_df.coalesce(1).write.mode('overwrite').format('csv') \
        .option('header', 'true') \
        .save(output+'/'+'donation_data_for_histogram')

    spark.stop()


def files_from_args():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', default='s3://rw-cs696-data/itcont.txt')
    parser.add_argument('-dh', '--header', default='s3://ss-cs696-assignment4/assignment4_data/indiv_header_file.csv')
    parser.add_argument('-o', '--output', default='s3://ss-cs696-assignment4/output')
    args = parser.parse_args()
    return args.data, args.header, args.output


if __name__ == "__main__":
    election_data_file, election_data_header_file, output = files_from_args()
    election_campaign_data(election_data_file, election_data_header_file, output)
