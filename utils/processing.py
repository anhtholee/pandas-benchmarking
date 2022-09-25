def find_str_df(review):
    return review.str.extract(r"\$(\d{1,3}(?:\,\d{3})*)\b")