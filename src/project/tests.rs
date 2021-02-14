


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
        id: "table.s.obj_0".into(),
        path_buf: "s.obj_0".into(),
        script: "create s.obj_0;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_1", "table.s.obj_2"]),
        required_by: set(vec![]),
    };

    let obj_1 = DatabaseObject {
        id: "table.s.obj_1".into(),
        path_buf: "s.obj_1".into(),
        script: "create s.obj_1;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_2", "table.s.obj_3"]),
        required_by: set(vec!["table.s.obj_0"]),
    };

    let obj_2 = DatabaseObject {
        id: "table.s.obj_2".into(),
        path_buf: "s.obj_2".into(),
        script: "create s.obj_2;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_3"]),
        required_by: set(vec!["table.s.obj_1", "table.s.obj_0"]),
    };

    let obj_3 = DatabaseObject {
        id: "table.s.obj_3".into(),
        path_buf: "s.obj_3".into(),
        script: "create s.obj_3;".into(),
        md5: "asd".into(),
        depends_on: set(vec![]),
        required_by: set(vec!["table.s.obj_1", "table.s.obj_2"]),
    };

    let mut objects: HashMap<String, DatabaseObject> = HashMap::new();
    objects.insert(obj_0.id.clone(), obj_0);
    objects.insert(obj_1.id.clone(), obj_1);
    objects.insert(obj_2.id.clone(), obj_2);
    objects.insert(obj_3.id.clone(), obj_3);



    let create_order_expected: Vec<String> = vec![
        "table.s.obj_3".into(),
        "table.s.obj_2".into(),
        "table.s.obj_1".into(),
        "table.s.obj_0".into(),
    ];
    let create_order_actual = calc_create_order(&objects)?;

    assert_eq!(create_order_actual, create_order_expected);
    return Ok(());
}



#[test]
fn test_calc_execute_order_1() -> anyhow::Result<()> {
    
    let obj_0 = DatabaseObject {
        id: "table.s.obj_0".into(),
        path_buf: "s.obj_0".into(),
        script: "create s.obj_0;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_1"]),
        required_by: set(vec!["table.s.obj_3"]),
    };

    let obj_1 = DatabaseObject {
        id: "table.s.obj_1".into(),
        path_buf: "s.obj_1".into(),
        script: "create s.obj_1;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_2"]),
        required_by: set(vec!["table.s.obj_0"]),
    };

    let obj_2 = DatabaseObject {
        id: "table.s.obj_2".into(),
        path_buf: "s.obj_2".into(),
        script: "create s.obj_2;".into(),
        md5: "asd".into(),
        depends_on: set(vec!["table.s.obj_0"]),
        required_by: set(vec!["table.s.obj_1"]),
    };

    let obj_3 = DatabaseObject {
        id: "table.s.obj_3".into(),
        path_buf: "s.obj_3".into(),
        script: "create s.obj_3;".into(),
        md5: "asd".into(),
        depends_on: set(vec![]),
        required_by: set(vec![]),
    };

    let mut objects: HashMap<String, DatabaseObject> = HashMap::new();
    objects.insert(obj_0.id.clone(), obj_0);
    objects.insert(obj_1.id.clone(), obj_1);
    objects.insert(obj_2.id.clone(), obj_2);
    objects.insert(obj_3.id.clone(), obj_3);


    let execute_order_actual = calc_create_order(&objects);
    assert_eq!(execute_order_actual.is_err(), true);

    return Ok(());
}