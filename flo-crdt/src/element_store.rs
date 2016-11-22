use ::{ActorId, Dot};
use element::{Element, OwnedElement};
use version_map::VersionMap;

#[derive(Debug, PartialEq)]
pub struct ElementStore { //TODO: change ElementStore to a trait
    elements: Vec<OwnedElement>
}

impl ElementStore {
    pub fn new() -> ElementStore {
        ElementStore {
            elements: Vec::new(),
        }
    }

    pub fn add_element(&mut self, element: OwnedElement) {
        self.elements.push(element);
        self.elements.sort_by_key(|e| e.id);
    }

    pub fn count(&self) -> usize {
        self.elements.len()
    }

    pub fn iter_range<'a>(&'a self, start: Dot) -> impl Iterator<Item=&'a OwnedElement> {
        self.elements.iter().filter(move |element| {
            element.id > start
        })
    }

    pub fn get_delta<'a, T: VersionMap>(&'a self, other_node: ActorId, version_vec: &'a T) -> impl Iterator<Item=&'a OwnedElement> + 'a {
        self.elements.iter().filter(move |element| {
            let element_actor = element.id.actor;
            element_actor != other_node && element.id.counter > version_vec.get_element_counter(element_actor).unwrap_or(0)
        })
    }
}

