import os
import json
import random
from client import CRMDataPipelineEnvClient
from models import PipelineActionType, CRMPipelineAction

class QTableAgent:
    """A simple Q-Table agent to demonstrate RL training loop."""
    def __init__(self, action_types, alpha=0.1, gamma=0.9, epsilon=0.2):
        self.q_table = {}  # (state_repr, action_type) -> value
        self.action_types = action_types
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon

    def get_state_repr(self, obs):
        # Discretize state for tabular RL
        sources = ",".join(sorted(obs.available_sources))
        has_report = "Y" if obs.data_quality_report else "N"
        return f"{sources}|{has_report}"

    def choose_action(self, state_repr):
        if random.random() < self.epsilon:
            return random.choice(self.action_types)
        
        q_values = [self.q_table.get((state_repr, a), 0.0) for a in self.action_types]
        max_q = max(q_values)
        actions_with_max_q = [a for a, q in zip(self.action_types, q_values) if q == max_q]
        return random.choice(actions_with_max_q)

    def learn(self, state, action, reward, next_state):
        old_value = self.q_table.get((state, action), 0.0)
        next_max = max([self.q_table.get((next_state, a), 0.0) for a in self.action_types])
        new_value = old_value + self.alpha * (reward + self.gamma * next_max - old_value)
        self.q_table[(state, action)] = new_value

def train(task_id="t1", episodes=10):
    base_url = os.environ.get("OPENENV_BASE_URL", "http://localhost:8080")
    print(f"\n--- Starting RL Training on {task_id} ({episodes} episodes) ---")
    
    action_types = [
        PipelineActionType.PROFILE_SOURCE,
        PipelineActionType.STANDARDIZE_COLUMN,
        PipelineActionType.DEDUPLICATE,
        PipelineActionType.SUBMIT_PIPELINE
    ]
    
    agent = QTableAgent(action_types)
    history = []

    try:
        with CRMDataPipelineEnvClient(base_url=base_url).sync() as env:
            for ep in range(episodes):
                result = env.reset(task_id=task_id)
                done = False
                total_reward = 0
                steps = 0
                
                state_repr = agent.get_state_repr(result.observation)
                
                while not done and steps < 10:
                    if not result.observation.available_sources:
                        break
                    
                    action_type = agent.choose_action(state_repr)
                    primary_source = result.observation.available_sources[0]
                    
                    # Construct valid action based on type
                    action_payload = {"action_type": action_type}
                    if action_type == PipelineActionType.PROFILE_SOURCE:
                        action_payload["source"] = primary_source
                    elif action_type == PipelineActionType.STANDARDIZE_COLUMN:
                        action_payload["source"] = primary_source
                        action_payload["column"] = "email"
                        action_payload["standardization_strategy"] = "LOWERCASE_STRIP"
                    elif action_type == PipelineActionType.DEDUPLICATE:
                        action_payload["source"] = primary_source
                        action_payload["deduplication_strategy"] = "EXACT_EMAIL"
                    elif action_type == PipelineActionType.SUBMIT_PIPELINE:
                        action_payload["final_source"] = primary_source

                    action = CRMPipelineAction(**action_payload)
                    result = env.step(action)
                    
                    next_state_repr = agent.get_state_repr(result.observation)
                    reward = result.reward
                    
                    agent.learn(state_repr, action_type, reward, next_state_repr)
                    
                    state_repr = next_state_repr
                    total_reward += reward
                    done = result.done
                    steps += 1
                
                history.append(total_reward)
                if (ep + 1) % 5 == 0:
                    avg_reward = sum(history[-5:]) / 5
                    print(f"  Episode {ep+1}/{episodes} | Avg Reward: {avg_reward:.4f}")

    except Exception as e:
        print(f"  [ERROR] Training failed: {e}")
        import traceback; traceback.print_exc()

    print(f"\nTraining Complete. Initial Reward: {history[0]:.4f} | Final Reward: {history[-1]:.4f}")
    return history

if __name__ == "__main__":
    # Small test run
    train("t1", episodes=20)
