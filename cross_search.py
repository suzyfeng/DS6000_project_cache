# sampled_cmt_test= sampled_comments.select("id","author","link_id","parent_id",'body')
# sampled_sub_test= sampled_submissions.select("author",'id',"created_utc","title",'num_comments')

# sampled_cmt_test = sampled_cmt_test.groupBy('link_id').agg(F.concat_ws('\n', F.collect_list('body')).alias('joined_body'))
# sampled_cmt_test.show()

# sampled_sub_test = sampled_sub_test.withColumnRenamed('id', 'submission_id')
# joined_df = sampled_sub_test.join(sampled_cmt_test, sampled_sub_test.submission_id == sampled_cmt_test.link_id, "inner")
# joined_df.show()