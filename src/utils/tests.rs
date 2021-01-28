

use anyhow;
use super::*;



#[test]
fn test_contains_whole_word_ci() -> anyhow::Result<()> {    
    assert_eq!(contains_whole_word_ci("asdf", "asdf"), true);
    assert_eq!(contains_whole_word_ci("as.df", "as.df"), true);
    assert_eq!(contains_whole_word_ci("asdfe", "asdf"), false);
    assert_eq!(contains_whole_word_ci("easdf", "asdf"), false);
    assert_eq!(contains_whole_word_ci("qwe as_df.qw_we qwr", "as_df.qw_we"), true);
    assert_eq!(contains_whole_word_ci("qwe as_df qw_we qwr", "as_df.qw_we"), false);
    assert_eq!(contains_whole_word_ci("qwe as_df.qw_wez qwr", "as_df.qw_we"), false);
    assert_eq!(contains_whole_word_ci("aSdf", "asdf"), true);
    assert_eq!(contains_whole_word_ci("aSdf", "asdF"), true);
    assert_eq!(contains_whole_word_ci("AS.DF", "as.df"), true);
    assert_eq!(contains_whole_word_ci("as.df", "AS.DF"), true);
    assert_eq!(contains_whole_word_ci("qwe AS_DF.QW_WE qwr", "AS_DF.QW_WE"), true);
    assert_eq!(contains_whole_word_ci("qwe as_df.Qw_we qwr", "as_df.qw_we"), true);
    assert_eq!(contains_whole_word_ci("qwe as_df.qw_we qwr", "As_df.qw_we"), true);
    assert_eq!(contains_whole_word_ci("", ""), true);
    assert_eq!(contains_whole_word_ci(".", ""), true);
    assert_eq!(contains_whole_word_ci("", "."), false);
    return Ok(());
}

#[test]
fn test_contains_whole_word() -> anyhow::Result<()> {    
    assert_eq!(contains_whole_word("asdf", "asdf"), true);
    assert_eq!(contains_whole_word("as.df", "as.df"), true);
    assert_eq!(contains_whole_word("asdfe", "asdf"), false);
    assert_eq!(contains_whole_word("easdf", "asdf"), false);
    assert_eq!(contains_whole_word("qwe as_df.qw_we qwr", "as_df.qw_we"), true);
    assert_eq!(contains_whole_word("qwe as_df qw_we qwr", "as_df.qw_we"), false);
    assert_eq!(contains_whole_word("qwe as_df.qw_wez qwr", "as_df.qw_we"), false);
    assert_eq!(contains_whole_word("aSdf", "asdf"), false);
    assert_eq!(contains_whole_word("aSdf", "asdF"), false);
    assert_eq!(contains_whole_word("AS.DF", "as.df"), false);
    assert_eq!(contains_whole_word("as.df", "AS.DF"), false);
    assert_eq!(contains_whole_word("qwe AS_DF.QW_WE qwr", "AS_DF.QW_WE"), true);
    assert_eq!(contains_whole_word("qwe as_df.Qw_we qwr", "as_df.qw_we"), false);
    assert_eq!(contains_whole_word("qwe as_df.qw_we qwr", "As_df.qw_we"), false);
    assert_eq!(contains_whole_word("", ""), true);
    assert_eq!(contains_whole_word(".", ""), true);
    assert_eq!(contains_whole_word("", "."), false);
    return Ok(());
}