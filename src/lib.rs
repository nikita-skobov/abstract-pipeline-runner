use yaml_rust::yaml::Yaml;
use crossbeam_utils::thread;

#[derive(Debug, Clone)]
pub enum NodeType<'a> {
    NodeTypeSeries(Vec<Node<'a>>),
    NodeTypeParallel(Vec<Node<'a>>),
    NodeTypeTask,
}
impl<'a> Default for NodeType<'a> {
    fn default() -> Self {
        NodeTypeTask
    }
}
pub use NodeType::*;

#[derive(Debug, Default, Clone)]
pub struct Node<'a> {
    pub is_root_node: bool,
    pub name: Option<&'a str>,
    pub ntype: NodeType<'a>,
    pub task_str: Option<&'a str>,

    // TODO: this should probably be generic
    pub captured_output: Option<&'a str>,
}

pub fn run_node<'a>(node: &Node<'a>) -> bool {
    match node.ntype {
        NodeTypeSeries(ref v) => {
            let mut success = true;
            for n in v.iter() {
                success = run_node(n);
            }
            success
        }
        NodeTypeParallel(ref v) => {
            thread::scope(|scope| {
                let handles = (0..v.len()).into_iter()
                    .map(|i| {
                        scope.spawn(move |_| {
                            run_node(&v[i])
                        })
                    });
                let mut success = true;
                let collected = handles.collect::<Vec<_>>();
                for c in collected {
                    success = match c.join() {
                        Ok(s) => s && s == success,
                        _ => false,
                    };
                }
                success
            }).unwrap()
        }
        NodeTypeTask => {
            // TODO: be less successful
            let success = true;
            return success;
        }
    }
}

// #[derive(Debug, Default)]
// pub struct GlobalNodeContext<'a> {
//     pub root_node: Node<'a>,
// }
// impl<'a> GlobalNodeContext<'a> {
//     pub fn run() {

//     }
// }

pub trait Parser {
    fn get_top_level_names(&self) -> Vec<Option<&str>>;
    fn task_keyword_exists_in(&self, name: Option<&str>) -> bool;

    // default implementation assumes any node is the root node
    // because this method only called by the parser for top
    // level nodes, which will typically only be one... make a
    // custom implementation if you want
    fn should_be_root_node(&self, name: Option<&str>) -> bool {
        self.is_node(name)
    }
    // by default a node is a node if its name is the series
    // keyword, or parallel keyword, OR if the node's object
    // contains the task keyword
    fn is_node(&self, name: Option<&str>) -> bool {
        if let Some(ref n) = name {
            if *n == self.series_keyword() || *n == self.parallel_keyword() {
                true
            } else if self.task_keyword_exists_in(name) {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    fn task_keyword(&self) -> &str { "run" }
    fn series_keyword(&self) -> &str { "series" }
    fn parallel_keyword(&self) -> &str { "parallel" }
}

pub fn create_runner(parser: &impl Parser) {
    let mut root_node = None;
    let mut known_nodes = vec![];
    for name in parser.get_top_level_names() {
        if ! parser.is_node(name) { continue; }
        println!("GOT NODE: {:?}", name);

        if parser.should_be_root_node(name) {
            let mut node = Node::default();
            node.name = name;
            root_node = Some(node)
        } else {
            let mut node = Node::default();
            node.name = name;
            known_nodes.push(node);
        }
    }

    println!("ROOT NODE: {:?}", root_node);

}


// =======================================================
// this is a specific implementation and probably should
// not be in this project...
pub fn yaml_get_field_as_str(yaml: &Yaml) -> Option<&str> {
    match yaml {
        Yaml::Real(ref r) => Some(r),
        Yaml::String(ref s) => Some(s),
        Yaml::Null => Some("null"),
        _ => None,
        // not implemented:
        // Yaml::Integer(_) => {}
        // Yaml::Boolean(_) => {}
        // Yaml::Array(_) => {}
        // Yaml::Hash(_) => {}
        // Yaml::Alias(_) => {}
        // Yaml::BadValue => {}
    }
}

impl Parser for &Yaml {
    fn get_top_level_names(&self) -> Vec<Option<&str>>{
        if let Yaml::Hash(ref h) = self {
            let mut names = vec![];
            for (k, _) in h {
                names.push(yaml_get_field_as_str(k));
            }
            return names;
        } else if let Yaml::Array(ref a) = self {
            let mut names = vec![];
            for v in a {
                names.push(yaml_get_field_as_str(v));
            }
            return names;
        }
        // match self {
        //     Yaml::Hash(ref h) => {

        //     }
        //     Yaml::Real(_) => {}
        //     Yaml::Integer(_) => {}
        //     Yaml::String(_) => {}
        //     Yaml::Boolean(_) => {}
        //     Yaml::Array(_) => {}
        //     Yaml::Alias(_) => {}
        //     Yaml::Null => {}
        //     Yaml::BadValue => {}
        // }
        vec![]
    }
    fn task_keyword_exists_in(&self, name: Option<&str>) -> bool {
        // todo: what if its not in the root of the yaml?
        // need way to iterate over the name
        if let Some(ref n) = name {
            match self[*n] {
                Yaml::Hash(_) => {}
                Yaml::Real(_) => {}
                Yaml::Integer(_) => {}
                Yaml::String(_) => {}
                Yaml::Boolean(_) => {}
                Yaml::Array(_) => {}
                Yaml::Alias(_) => {}
                Yaml::Null => {}
                Yaml::BadValue => {}
            }
            println!("DOES TASK KEYWORD EXIST IN {} - {:?}", *n, self[*n]);
            true
        } else {
            false
        }
    }
}
// =======================================================

// #[cfg(test)]
// mod tests {
//     use super::*;
//     struct TestParser {}
//     struct TestParserOverride {}

//     impl Parser for TestParser {
//         fn get_top_level_names(&self) -> Vec<Option<&str>> { vec![] }
//         fn is_node(&self, name: Option<&str>) -> bool { false }
//     }
//     impl Parser for TestParserOverride {
//         fn get_top_level_names(&self) -> Vec<Option<&str>> { vec![] }
//         fn is_node(&self, name: Option<&str>) -> bool { false }
//         fn series_keyword(&self) -> &str { "my_series_kwd" }
//     }

//     fn setup() -> impl Parser {
//         let my_parser = TestParser {};
//         my_parser
//     }
//     fn setup_override() -> impl Parser {
//         let my_parser = TestParserOverride {};
//         my_parser
//     }

//     #[test]
//     fn uses_static_default_keywords() {
//         let parser = setup();
//         assert_eq!(parser.series_keyword(), "series");
//     }

//     #[test]
//     fn can_override_static_keywords() {
//         let parser = setup_override();
//         assert_eq!(parser.series_keyword(), "my_series_kwd");
//     }
// }
