use crossbeam_utils::thread;
use std::collections::HashMap;

/// an abstract concept of a single unit of work
/// can be implemented anyway you want, on any data structure
/// that you define a 'run' method for.
/// if you use Abstract Pipeline Runner for a code pipeline such as
/// GitHub actions, then you can think of a Task as what GitHub actions
/// refers to as 'steps'.
pub trait Task<T: Send + Sync + Clone>: Send + Sync {
    /// given the node and the current global context (for read only)
    /// run the task however you need to, then return a tuple of a success value
    /// and an optional ContextDiff if the caller needs to modify the global context
    fn run<U: Task<T> + Clone>(
        &self,
        node: &Node<T, U>,
        global_context: &GlobalContext<T, U>
    ) -> (bool, Option<Vec<ContextDiff>>);
}

/// enum describing the operation you wish to perform on
/// the global context once the current Task is complete
// can set a key, value, or remove a key value pair
// from the global context
#[derive(Debug, Clone)]
pub enum ContextDiff {
    CDSet(String, String),
    CDRemove(String),
}
pub use ContextDiff::*;

/// while the Node struct, and Task trait
/// are strongly abstract in the sense that you can
/// implement and structure your pipeline runner anyway you want,
/// unfortunately the GlobalContext is not very abstract.
/// it is currently just a hashmap of known_nodes
/// and variables. variables are what get set dynamically
/// by ContextDiff operations
#[derive(Default, Clone)]
pub struct GlobalContext<'a, T: Send + Sync + Clone, U: Task<T> + Clone> {
    pub known_nodes: HashMap<String, Node<'a, T, U>>,
    pub variables: HashMap<String, String>,
}
impl<'a, T: Send + Sync + Clone, U: Task<T> + Clone> GlobalContext<'a, T, U> {
    fn take_diff(&mut self, diff: ContextDiff) {
        match diff {
            CDSet(skey, sval) => {
                self.variables.insert(skey, sval);
            },
            CDRemove(skey) => {
                self.variables.remove(&skey);
            }
        }
    }
}

/// a node must be one of 3 types:
/// a container which contains a series of other nodes
/// a container which contains a parallel list of other nodes
/// a task which is a leaf node. the task node is the node that actually
/// contains information to give to the Task trait for it to
/// decide what to do.
#[derive(Clone)]
pub enum NodeType<'a, T: Send + Sync + Clone, U: Task<T> + Clone> {
    NodeTypeSeries(Vec<Node<'a, T, U>>),
    NodeTypeParallel(Vec<Node<'a, T, U>>),
    NodeTypeTask,
}
impl<'a, T: Send + Sync + Clone, U: Task<T> + Clone> Default for NodeType<'a, T, U> {
    fn default() -> Self {
        NodeTypeTask
    }
}
pub use NodeType::*;

/// a Node is a recursive data structure
/// it's only important fields are it's type
/// (series/parallel/task), it's properties hashmap,
/// a task field (only set to Some for nodes that have
/// type == NodeTypeTask), and continue_on_fail will
/// allow the node run recursive functions to continue
/// even if one of its nodes reports a failure
#[derive(Default, Clone)]
pub struct Node<'a, T: Send + Sync + Clone, U: Task<T> + Clone> {
    pub is_root_node: bool,
    pub name: Option<&'a str>,
    pub ntype: NodeType<'a, T, U>,
    pub task: Option<&'a U>,
    pub properties: HashMap<&'a str, T>,
    pub continue_on_fail: bool,
}

impl<'a, T: Send + Sync + Clone, U: Task<T> + Clone> Node<'a, T, U> {
    pub fn pretty_print(&self) -> String {
        let mut string = String::new();
        self.pretty_print_with_indent(&mut string, 0);
        string
    }

    fn pretty_print_with_indent(&self, current_string: &mut String, indent: usize) {
        let indent_size = 4;
        let indent_char = ' ';
        let mut indent_str = String::with_capacity(indent);
        for _ in 0..indent {
            indent_str.push(indent_char);
        }
        match &self.ntype {
            NodeTypeSeries(vec) => {
                let fmt_str = match self.name {
                    None => format!("{}Series\n", indent_str),
                    Some(n) => format!("{}Series ({})\n", indent_str, n),
                };
                current_string.push_str(fmt_str.as_str());
                for v in vec {
                    v.pretty_print_with_indent(current_string, indent + indent_size);
                }
            },
            NodeTypeParallel(vec) => {
                let fmt_str = match self.name {
                    None => format!("{}Parallel\n", indent_str),
                    Some(n) => format!("{}Parallel ({})\n", indent_str, n),
                };
                current_string.push_str(fmt_str.as_str());
                for v in vec {
                    v.pretty_print_with_indent(current_string, indent + indent_size);
                }
            },
            NodeTypeTask => {
                let fmt_str = match self.name {
                    None => format!("{}Task\n", indent_str),
                    Some(n) => format!("{}Task ({})\n", indent_str, n),
                };
                current_string.push_str(fmt_str.as_str());
                for _ in 0..indent_size {
                    indent_str.push(indent_char);
                }
                // TODO: cant fmt_str properties because its generic T
                // for prop in &self.properties {
                //     let fmt_str = format!("{}{}: {}\n", indent_str, prop.0, prop.1);
                //     current_string.push_str(fmt_str.as_str());
                // }
            }
        }
    }
}

// this is pretty ugly for nested structures because rust doesn't let you
// pretty format with indentation, so I use the above custom pretty_print() method
// its slow, and shouldn't be used in a real program, but useful for debugging
impl<'a, T: Send + Sync + Clone, U: Task<T> + Clone> std::fmt::Debug for Node<'a, T, U> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.ntype {
            NodeTypeSeries(vec) => {
                let mut out = f.debug_tuple("\nSeries\n");
                for v in vec {
                    out.field(v);
                }
                out.finish()
            },
            NodeTypeParallel(vec) => {
                let mut out = f.debug_tuple("\nSeries\n");
                for v in vec {
                    out.field(v);
                }
                out.finish()
            },
            NodeTypeTask => {
                let mut out = f.debug_struct("\nTask\n");
                if let Some(n) = self.name {
                    out.field("name", &n);
                }
                // TODO:
                // out.field("properties", &self.properties);
                out.finish()
            }
        }
    }
}

// a helper function that does the same as
// the `run_node_series` function, but it will apply each diff
// to the mut global context in addition to returning
// a vec of diffs. this mut global context is short lived, and only
// used so that serial nodes can see data from before each other
pub fn run_node_series_with_cloned_context<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    nodes: &Vec<Node<'a, T, U>>,
    mut_global_context: &'a mut GlobalContext<T, U>,
    continue_on_fail: bool,
) -> (bool, Option<Vec<ContextDiff>>) {
    let mut success = true;
    let mut diff_vec = vec![];
    for n in nodes.iter() {
        let (status, c) = run_node(n, &Some(mut_global_context), &mut None);
        if !status && !continue_on_fail {
            return (false, c);
        }
        if let Some(diffs) = c {
            for diff in diffs {
                mut_global_context.take_diff(diff.clone());
                diff_vec.push(diff);
            }
        }
        success = status;
    }

    let diff_vec_opt = if diff_vec.len() > 0 { Some(diff_vec) } else { None };
    (success, diff_vec_opt)
}

pub fn run_node_series<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    nodes: &Vec<Node<'a, T, U>>,
    global_context: &Option<&'a GlobalContext<T, U>>,
    mut_global_context: &mut Option<&'a mut GlobalContext<T, U>>,
    continue_on_fail: bool,
) -> (bool, Option<Vec<ContextDiff>>) {
    let mut success = true;
    let mut diff_vec = vec![];
    // if we dont have access to the mutable context
    // then we should make a copy of the immutable context
    // and use the helper function that will let serial nodes
    // see global context from each other
    if mut_global_context.is_none() {
        let unwrapped = global_context.unwrap();
        let mut mut_gc_clone = unwrapped.clone();
        return run_node_series_with_cloned_context(
            nodes,
            &mut mut_gc_clone,
            continue_on_fail,
        );
    }

    for n in nodes.iter() {
        let (status, c) = run_node(n, global_context, mut_global_context);
        if !status && !continue_on_fail {
            return (false, c);
        }
        if let Some(diffs) = c {
            for diff in diffs {
                diff_vec.push(diff);
            }
        }
        success = status;
    }
    let diff_vec_opt = if diff_vec.len() > 0 { Some(diff_vec) } else { None };
    (success, diff_vec_opt)
}

pub fn run_threads_with_context<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    nodes: &Vec<Node<'a, T, U>>,
    global_context: &Option<&'a GlobalContext<T, U>>,
) -> (bool, Option<Vec<ContextDiff>>) {
    thread::scope(|scope| {
        // TODO: maybe add max concurrent threads?
        let handles = (0..nodes.len()).into_iter()
            .map(|i| {
                scope.spawn(move |_| {
                    // concurrent threads cannot modify same
                    // memory location, so we pass none explicitly
                    // however, they will all return their diffs of
                    // what they want the state to be changed to
                    // and then we apply that diff when we collect it
                    let mut mut_none = None;
                    run_node(&nodes[i], &global_context, &mut mut_none)
                })
            });
        let mut diff_vec = vec![];
        let mut success = true;
        let collected = handles.collect::<Vec<_>>();
        for c in collected {
            success = match c.join() {
                Ok(s) => {
                    match s.1 {
                        None => (),
                        Some(diff_v) => {
                            for diff in diff_v {
                                diff_vec.push(diff);
                            }
                        }
                    }
                    s.0 && s.0 == success
                },
                _ => false,
            };
        }
        let diff_vec_opt = if diff_vec.len() > 0 { Some(diff_vec) } else { None };
        (success, diff_vec_opt)
    }).unwrap()
}

pub fn run_node_parallel<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    nodes: &Vec<Node<'a, T, U>>,
    global_context: &Option<&'a GlobalContext<T, U>>,
    mut_global_context: &mut Option<&'a mut GlobalContext<T, U>>,
) -> (bool, Option<Vec<ContextDiff>>) {
    // two cases here: we either are given a mutable, or immutable global context
    if let Some(mgc) = mut_global_context {
        // if mutable, we modify the global context ourselves
        // (id like this happen during the thread joining, ie: while its
        // still proecssing other threads potentially, but im not sure its possible)
        let mgc_deref = &*mgc;
        let gc: &GlobalContext<T, U> = mgc_deref;
        let gc_opt = Some(gc);
        let values = run_threads_with_context(nodes, &gc_opt);

        let (success, diff_vec) = values;
        if let Some(diff_vec) = diff_vec {
            for diff in diff_vec {
                mgc.take_diff(diff);
            }
        }
        (success, None)
    } else {
        // otherwise,
        // we just return a vec of diffs for our caller to apply
        run_threads_with_context(nodes, global_context)
    }
}

pub fn run_node_task<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    node: &Node<'a, T, U>,
    global_context: &Option<&'a GlobalContext<T, U>>,
    mut_global_context: &mut Option<&'a mut GlobalContext<T, U>>,
) -> (bool, Option<Vec<ContextDiff>>) {
    match node.task {
        None => (false, None),
        Some(cb) => {
            let (success, context_diff) = if let Some(mgc) = mut_global_context {
                cb.run(node, *mgc)
            } else if let Some(gc) = global_context {
                cb.run(node, *gc)
            } else {
                panic!("unsupported usage")
            };

            // if the task said there is nothing to modify,
            // then just return the success flag
            // or if there is a context to modify, but our caller
            // did not give us a mutable context to modify, then
            // return the diff, and let the caller handle applying it
            if context_diff.is_none() || mut_global_context.is_none() {
                return (success, context_diff)
            }

            // if the task provides a global context
            // diff, and our caller allows us to modify the
            // mut_global_context, then we 'apply' that
            // diff to the actual global context.
            if let Some(mgc) = mut_global_context {
                let context_diff = context_diff.unwrap();
                for diff in context_diff {
                    mgc.take_diff(diff);
                }
            }

            // we return none because we were allowed to
            // modify the global context above,
            // so no point in telling our caller to modify for us
            (success, None)
        },
    }
}

/// this should really be private, but I thought maybe I might need it to be public
/// at some point ¯\_(ツ)_/¯
/// run_node is a recursive fn that will take a context (both as a mutable reference
/// and immutable reference which point to the same thing), and iterate through
/// the node higherarchy and on each leaf node (that is a task node) it will
/// do node.task.run() and modify the global context if mutable, otherwise
/// it will return a vector of diffs to the caller for the caller to apply
/// if/as needed
pub fn run_node<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    node: &Node<'a, T, U>,
    global_context: &Option<&'a GlobalContext<T, U>>,
    mut_global_context: &mut Option<&'a mut GlobalContext<T, U>>,
) -> (bool, Option<Vec<ContextDiff>>)
{
    match node.ntype {
        NodeTypeSeries(ref v) => {
            run_node_series(v, global_context, mut_global_context, node.continue_on_fail)
        }
        NodeTypeParallel(ref v) => {
            run_node_parallel(v, global_context, mut_global_context)
        }
        NodeTypeTask => {
            run_node_task(node, global_context, mut_global_context)
        }
    }
}

/// this is a public helper to call `run_node`. it takes a single
/// mutable refernece to a global context, and calls run_node with that reference
/// as both mutable, and imutable (because run_node needs access to a mutable in some cases
/// and immutable in others)
/// see docs for `run_node` for more details
pub fn run_node_helper<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    node: &Node<'a, T, U>,
    global_context: &'a mut GlobalContext<T, U>,
) -> (bool, Option<Vec<ContextDiff>>)
{
    let none = None;
    let mut some = Some(global_context);
    run_node(
        node,
        &none,
        &mut some,
    )
}

/// this is a public helper to call `run_node`. it is like `run_node_helper`
/// but only takes an immutable global context
pub fn run_node_helper_immut<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
    node: &Node<'a, T, U>,
    global_context: &'a GlobalContext<T, U>,
) -> (bool, Option<Vec<ContextDiff>>)
{
    let mut none = None;
    let some = Some(global_context);
    run_node(
        node,
        &some,
        &mut none,
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use std::time::Instant;
    use std::thread::sleep;


    // =================================
    // this block of code was taken from
    // https://github.com/dtolnay/dyn-clone/blob/master/src/lib.rs
    // the license of which is MIT and is here: https://github.com/dtolnay/dyn-clone/blob/master/LICENSE-MIT
    // this code was copy pasted as is because I didn't want a dependency
    // just for this test. the actual library above does not need any
    // dependencies, but this DynClone is convenient for testing
    pub trait DynClone {
        unsafe fn clone_box(&self) -> *mut ();
    }
    pub fn clone<T>(t: &T) -> T where T: DynClone {
        unsafe {
            *Box::from_raw(<T as DynClone>::clone_box(t) as *mut T)
        }
    }
    pub fn clone_box<T>(t: &T) -> Box<T> where T: ?Sized + DynClone {
        let mut fat_ptr = t as *const T;
        unsafe {
            let data_ptr = &mut fat_ptr as *mut *const T as *mut *mut ();
            assert_eq!(*data_ptr as *const (), t as *const T as *const ());
            *data_ptr = <T as DynClone>::clone_box(t);
        }
        unsafe {
            Box::from_raw(fat_ptr as *mut T)
        }
    }
    impl<T> DynClone for T where T: core::clone::Clone {
        unsafe fn clone_box(&self) -> *mut () {
            Box::into_raw(Box::new(self.clone())) as *mut ()
        }
    }
    trait FnClone: DynClone {
        fn call(&self) -> bool;
    }
    impl<F> FnClone for F where F: Fn() -> bool + Clone {
        fn call(&self) -> bool {
            self()
        }
    }
    // =================================

    // use FnClone: DynClone
    // because by default Rust cannot clone a Box<dyn Fn()>
    // and for testing it would be convenient to use a cloned closure
    struct MyTask
    {
        cb: Box<dyn FnClone + Send + Sync>,
    }
    impl Clone for MyTask {
        fn clone(&self) -> Self {
            MyTask {
                cb: clone_box(&*self.cb)
            }
        }
    }

    impl MyTask{
        fn do_cb(&self) -> bool {
            let cb = &self.cb;
            cb.call()
        }
    }

    impl<T: Send + Sync + Clone> Task<T> for MyTask {
        fn run<U: Task<T> + Clone>(&self, node: &Node<T, U>, global_context: &GlobalContext<T, U>) ->
            (bool, Option<Vec<ContextDiff>>)
        {
            let out_diff = if let Some(s) = node.name  {
                Some(vec![CDSet(s.into(), "".into())])
            } else {
                None
            };
            (self.do_cb(), out_diff)
        }
    }

    fn make_root_node_with_list<'a, T: Send + Sync + Clone, U: Task<T> + Clone>(
        series_size: usize,
        task: &'a U,
        is_series: bool,
        name_vec: &'a mut Vec<&str>,
    ) -> Node<'a, T, U> {
        let mut root = Node {
            name: None,
            is_root_node: false,
            ntype: NodeTypeTask,
            task: None,
            properties: HashMap::new(),
            continue_on_fail: false,
        };
        let mut node_vec = vec![];
        for i in 0..series_size {
            let mut task_node = Node {
                name: None,
                is_root_node: false,
                ntype: NodeTypeTask,
                task: None,
                properties: HashMap::new(),
                continue_on_fail: false,
            };
            task_node.ntype = NodeTypeTask;
            if name_vec.len() > i {
                task_node.name = Some(name_vec[i]);
            }
            task_node.task = Some(task);
            node_vec.push(task_node);
        }
        if is_series {
            root.ntype = NodeTypeSeries(node_vec);
        } else {
            root.ntype = NodeTypeParallel(node_vec);
        }
        root
    }

    #[test]
    fn returns_true_if_task_successful() {
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mut root = Node {
            name: None,
            is_root_node: false,
            ntype: NodeTypeTask,
            task: None,
            properties: HashMap::new(),
            continue_on_fail: false,
        };
        root.ntype = NodeTypeTask;
        root.task = Some(&mytask);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);
    }

    #[test]
    fn returns_false_if_task_fails() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, MyTask>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| false) };
        let mut root = Node {
            name: None,
            is_root_node: false,
            ntype: NodeTypeTask,
            task: None,
            properties: HashMap::new(),
            continue_on_fail: false,
        };
        root.ntype = NodeTypeTask;
        root.task = Some(&mytask);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, false);
    }

    #[test]
    fn returns_true_if_all_tasks_successful_in_series() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut strvec = vec![];
        let root = make_root_node_with_list(3, &mytask, true, &mut strvec);
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);
    }

    #[test]
    fn returns_false_if_one_tasks_fails_in_series() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let mut root = make_root_node_with_list(5, &mytask, true, &mut strvec);

        // the third task should fail, so the global
        // context should not have d because d never gets ran
        let myfailtask = MyTask { cb: Box::new(|| false) };
        if let NodeTypeSeries(ref mut s) = root.ntype {
            s[2].task = Some(&myfailtask);
        }
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, false);

        // currently this sets the global context to have 'c',
        // but should it? if it failed on 'c', should it be allowed
        // to modify context?
        assert!(mycontext.variables.contains_key("a"));
        assert!(mycontext.variables.contains_key("b"));
    }

    #[test]
    fn returns_true_if_all_tasks_succeed_in_parallel() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let root = make_root_node_with_list(5, &mytask, false, &mut strvec);

        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);
    }

    #[test]
    fn returns_false_if_one_tasks_fails_in_parallel() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let mut root = make_root_node_with_list(5, &mytask, false, &mut strvec);

        let myfailtask = MyTask { cb: Box::new(|| false) };
        if let NodeTypeParallel(ref mut s) = root.ntype {
            s[2].task = Some(&myfailtask);
        }
        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, false);
    }

    #[test]
    fn parallel_is_not_a_liar() {
        // run in series with a 0.1 second delay on 5 items
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| {
            sleep(Duration::from_millis(100));
            true
        }) };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let root = make_root_node_with_list(5, &mytask, true, &mut strvec);
        let mut timer = Instant::now();
        let (result, _) = run_node_helper(&root, &mut mycontext);
        let duration = timer.elapsed().as_millis();
        assert_eq!(result, true);
        assert!(duration >= 500);

        // now do the same but in parallel. the duration should
        // be about 100ms
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| {
            sleep(Duration::from_millis(100));
            true
        }) };
        let mut strvec = vec!["a", "b", "c", "d", "e"];
        let root = make_root_node_with_list(5, &mytask, false, &mut strvec);
        let mut timer = Instant::now();
        let (result, _) = run_node_helper(&root, &mut mycontext);
        let duration = timer.elapsed().as_millis();
        assert_eq!(result, true);
        assert!(duration >= 100);
        assert!(duration < 400);
    }

    // basically we test the functionality of the context diff
    // being applied properly regardless of nesting of series/parallel nodes
    // so this node hierarchy looks like:
    // parallel:
    //   - parallel:
    //       - series:
    //          - parallel: [p1, p2, p3]
    //          - s2
    //          - s3
    //       - b
    //       - c
    //       - d
    //       - e
    //   - parallel: [f, g, h, i, j]
    // we test that regardless of the nesting, all of those keys will
    // be applied to the global context
    #[test]
    fn nested_parallels_and_series_can_still_modify_context() {
        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask { cb: Box::new(|| true) };
        let mut strvec1 = vec!["", "b", "c", "d", "e"];
        let mut strvec2 = vec!["f", "g", "h", "i", "j"];
        let mut strvec3 = vec!["", "s2", "s3"];
        let mut strvec4 = vec!["p1", "p2", "p3"];
        let mut parvec = vec!["", ""];
        let mut inner_parallel1 = make_root_node_with_list(5, &mytask, false, &mut strvec1);
        let inner_parallel2 = make_root_node_with_list(5, &mytask, false, &mut strvec2);
        let inner_parallel3 = make_root_node_with_list(3, &mytask, false, &mut strvec4);
        let mut inner_series1 = make_root_node_with_list(3, &mytask, true, &mut strvec3);
        let mut root = make_root_node_with_list(2, &mytask, false, &mut parvec);

        if let NodeTypeSeries(ref mut s) = inner_series1.ntype {
            s[0] = inner_parallel3;
        }
        if let NodeTypeParallel(ref mut p) = inner_parallel1.ntype {
            p[0] = inner_series1;
        }
        if let NodeTypeParallel(ref mut p) = root.ntype {
            p[0] = inner_parallel1;
            p[1] = inner_parallel2;
        }

        let (result, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(result, true);

        // all of the characters in the strvecs above
        // should be present
        assert!(mycontext.variables.contains_key("b"));
        assert!(mycontext.variables.contains_key("c"));
        assert!(mycontext.variables.contains_key("d"));
        assert!(mycontext.variables.contains_key("e"));
        assert!(mycontext.variables.contains_key("f"));
        assert!(mycontext.variables.contains_key("g"));
        assert!(mycontext.variables.contains_key("h"));
        assert!(mycontext.variables.contains_key("i"));
        assert!(mycontext.variables.contains_key("j"));
        assert!(mycontext.variables.contains_key("s2"));
        assert!(mycontext.variables.contains_key("s3"));
        assert!(mycontext.variables.contains_key("p1"));
        assert!(mycontext.variables.contains_key("p2"));
        assert!(mycontext.variables.contains_key("p3"));
    }

    // series:
    //   - a
    //   - b
    #[test]
    fn context_diffs_applied_for_series() {
        #[derive(Clone)]
        struct MyTask1 {}
        impl<T: Send + Sync + Clone> Task<T> for MyTask1
        {
            fn run<U: Task<T> + Clone>(&self, node: &Node<T, U>, global_context: &GlobalContext<T, U>) ->
                (bool, Option<Vec<ContextDiff>>)
            {
                let has_a = format!("gc_has_a_{}", global_context.variables.contains_key("a"));
                let var_value = has_a;
                let out_diff = if let Some(s) = node.name {
                    Some(vec![CDSet(s.into(), var_value)])
                } else {
                    None
                };
                (true, out_diff)
            }
        }

        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask1 {};
        let mut parvec1 = vec!["a"];
        let mut parvec2 = vec!["b"];
        let mut servec = vec!["", ""];
        let inner_parallel1 = make_root_node_with_list(1, &mytask, false, &mut parvec1);
        let inner_parallel2 = make_root_node_with_list(1, &mytask, false, &mut parvec2);
        let mut root = make_root_node_with_list(2, &mytask, true, &mut servec);

        if let NodeTypeSeries(ref mut s) = root.ntype {
            s[0] = inner_parallel1;
            s[1] = inner_parallel2;
        }

        let (_, _) = run_node_helper(&root, &mut mycontext);
        assert_eq!(mycontext.variables["a"], "gc_has_a_false");
        // the b node should have access to a because it happens in series after a
        assert_eq!(mycontext.variables["b"], "gc_has_a_true");
    }

    // parallel:
    //    series:
    //       - parallel: [a,b,c]
    //       - parallel: [x,y,z]
    // since paralle [abc] and [xyz] are inside a series
    // that means all of the xyz tasks should be able to see
    // the output of the abc tasks because they were completed before them
    #[test]
    fn context_diffs_applied_for_nested_parallel() {
        #[derive(Clone)]
        struct MyTask1 {}
        impl<T: Send + Sync + Clone> Task<T> for MyTask1
        {
            fn run<U: Task<T> + Clone>(&self, node: &Node<T, U>, global_context: &GlobalContext<T, U>) ->
                (bool, Option<Vec<ContextDiff>>)
            {
                println!("RUNNING ON NODE: {:?}", node.name);
                println!("GLOBAL CONTEXT: {:?}", global_context.variables);
                let has_a = format!("gc_has_a_{}", global_context.variables.contains_key("a"));
                let has_b = format!("gc_has_b_{}", global_context.variables.contains_key("b"));
                let has_c = format!("gc_has_c_{}", global_context.variables.contains_key("c"));
                let var_value = format!("{},{},{}", has_a, has_b, has_c);
                let out_diff = if let Some(s) = node.name {
                    Some(vec![CDSet(s.into(), var_value)])
                } else {
                    None
                };
                (true, out_diff)
            }
        }

        let mut mycontext = GlobalContext {
            known_nodes: HashMap::<String, Node<&str, _>>::new(),
            variables: HashMap::new(),
        };
        let mytask = MyTask1 {};
        let mut parvec1 = vec!["a", "b", "c"];
        let mut parvec2 = vec!["x", "y", "z"];
        let mut servec = vec!["", ""];
        let mut parvec = vec![""];
        let inner_parallel1 = make_root_node_with_list(3, &mytask, false, &mut parvec1);
        let inner_parallel2 = make_root_node_with_list(3, &mytask, false, &mut parvec2);
        let mut inner_series1 = make_root_node_with_list(2, &mytask, true, &mut servec);
        let mut root = make_root_node_with_list(1, &mytask, false, &mut parvec);

        if let NodeTypeSeries(ref mut s) = inner_series1.ntype {
            s[0] = inner_parallel1;
            s[1] = inner_parallel2;
        }
        if let NodeTypeParallel(ref mut p) = root.ntype {
            p[0] = inner_series1;
        }

        let (result, diffs) = run_node_helper(&root, &mut mycontext);
        // println!("CONTEXT: {:?}", mycontext.variables);
        // all of the paralel [xyz] should have access to the output of the [abc] node
        // before it because that node happens in series with the xyz one
        assert_eq!(mycontext.variables["x"], "gc_has_a_true,gc_has_b_true,gc_has_c_true");
        assert_eq!(mycontext.variables["y"], "gc_has_a_true,gc_has_b_true,gc_has_c_true");
        assert_eq!(mycontext.variables["z"], "gc_has_a_true,gc_has_b_true,gc_has_c_true");
        // also all of the [abc] node should not have true for any of themselves because
        // they happen in parallel with each other
        assert_eq!(mycontext.variables["a"], "gc_has_a_false,gc_has_b_false,gc_has_c_false");
        assert_eq!(mycontext.variables["b"], "gc_has_a_false,gc_has_b_false,gc_has_c_false");
        assert_eq!(mycontext.variables["c"], "gc_has_a_false,gc_has_b_false,gc_has_c_false");
    }
}
