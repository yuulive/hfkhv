


use anyhow;
use super::*;


fn set(strings: Vec<&str>) -> HashSet<String> {
    let mut result = HashSet::new();
    for s in strings {
        result.insert(s.into());
    }
    return result;
}


#[test]
fn test_calc_execute_order_0() -> anyhow::Result<()> {
    
    let obj_0 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_0".into(),
        path_buf: "obj_0".into(),
        script: "create obj_0;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_1", "obj_2"]),
        required_by: set(vec![]),
    };

    let obj_1 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_1".into(),
        path_buf: "obj_1".into(),
        script: "create obj_1;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_2", "obj_3"]),
        required_by: set(vec!["obj_0"]),
    };

    let obj_2 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_2".into(),
        path_buf: "obj_2".into(),
        script: "create obj_2;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_3"]),
        required_by: set(vec!["obj_1", "obj_0"]),
    };

    let obj_3 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_3".into(),
        path_buf: "obj_3".into(),
        script: "create obj_3;".into(),
        md5: "asd".into(),
        depends_on: set(vec![]),
        required_by: set(vec!["obj_1", "obj_2"]),
    };

    let mut objects: HashMap<String, DatabaseObject> = HashMap::new();
    objects.insert("obj_0".into(), obj_0);
    objects.insert("obj_1".into(), obj_1);
    objects.insert("obj_2".into(), obj_2);
    objects.insert("obj_3".into(), obj_3);



    let execute_order_expected: Vec<String> = vec![
        "obj_3".into(),
        "obj_2".into(),
        "obj_1".into(),
        "obj_0".into(),
    ];
    let execute_order_actual = calc_execute_order(&objects)?;

    assert_eq!(execute_order_actual, execute_order_expected);
    return Ok(());
}




#[test]
fn test_calc_execute_order_1() -> anyhow::Result<()> {
    
    let obj_0 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_0".into(),
        path_buf: "obj_0".into(),
        script: "create obj_0;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_1"]),
        required_by: set(vec!["obj_3"]),
    };

    let obj_1 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_1".into(),
        path_buf: "obj_1".into(),
        script: "create obj_1;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_2"]),
        required_by: set(vec!["obj_0"]),
    };

    let obj_2 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_2".into(),
        path_buf: "obj_2".into(),
        script: "create obj_2;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["obj_0"]),
        required_by: set(vec!["obj_1"]),
    };

    let obj_3 = DatabaseObject {
        object_type: DatabaseObjectType::Table,
        id: "obj_3".into(),
        path_buf: "obj_3".into(),
        script: "create obj_3;".into(),
        md5: "asd".into(),
        depends_on: set(vec![]),
        required_by: set(vec![]),
    };

    let mut objects: HashMap<String, DatabaseObject> = HashMap::new();
    objects.insert("obj_0".into(), obj_0);
    objects.insert("obj_1".into(), obj_1);
    objects.insert("obj_2".into(), obj_2);
    objects.insert("obj_3".into(), obj_3);


    let execute_order_actual = calc_execute_order(&objects);
    assert_eq!(execute_order_actual.is_err(), true);

    return Ok(());
}